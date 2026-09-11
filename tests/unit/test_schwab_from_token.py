"""The real vendor's token-file factory, ``SchwabVendor.from_token``.

The design has the daemon rebuild its client from ``token.json`` at the top of every
cycle. That is what makes "capture resumes the instant re-auth happens" true. The
refresh token dies every seven days. A mid-week browser re-login rewrites the file at
the same path, and the next cycle must build a client from the new contents. If the
factory read the file once and cached the result, the daemon would keep the dead client
until someone restarted it, and every minute until then is a 401 gap.

Nothing in the suite drove this factory before. Every test that reaches capture replaces
``SchwabVendor`` with a stub, because the real factory builds a live ``schwab-py`` client
and no test has a token. So a cache added inside the factory would satisfy the whole
suite and still break re-auth recovery. These tests close that hole.

The client build is faked at its seam, ``schwab.auth.client_from_token_file``, not at the
factory. So the real ``from_token`` runs: it reads the path, forwards the secrets, and
wraps whatever the seam returns. The fake seam reads the token file it is handed and
returns a client whose mint time is the file's ``creation_timestamp``. So "built from the
file" is observable through the public API, because ``token_mint_time`` reads that value
back. A rewrite of the file changes that value, which is exactly the re-auth event the
design promises to notice.

The seam is installed as a fake ``schwab`` package in ``sys.modules`` rather than by
patching the real module's attribute. ``from_token`` imports ``schwab-py`` lazily so the
unit suite runs without the library installed, and this test keeps that true: it never
imports the real package, so it passes whether or not ``schwab-py`` is present.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType

import pytest

from lake.schwab import SchwabVendor
from tests.support.schwab import FakeSchwabClient

# Two mint epochs a week apart, the refresh token's real lifetime. The first stands for
# the token capture started on. The second stands for a mid-week re-login rewriting the
# same file. No wall clock is read.
MINT_BEFORE = 1787529900.0
MINT_AFTER = MINT_BEFORE + 7 * 86400  # one refresh-token lifetime later


class _RecordingFactory:
    """A stand-in for ``schwab.auth.client_from_token_file``.

    It reads the token file it is handed and builds a fake client whose mint time is the
    file's ``creation_timestamp``. So the client it returns is built from the file in the
    one way the vendor later observes. Every call is recorded, so a test can assert the
    factory ran once per ``from_token`` rather than serving a cache.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str, bool]] = []

    def __call__(
        self, token_path: str, api_key: str, app_secret: str, *, enforce_enums: bool = True
    ) -> FakeSchwabClient:
        contents = json.loads(Path(token_path).read_text())
        self.calls.append((token_path, api_key, app_secret, enforce_enums))
        return FakeSchwabClient(creation_timestamp=contents["creation_timestamp"])


def _install_seam(monkeypatch: pytest.MonkeyPatch, factory: _RecordingFactory) -> None:
    """Put a fake ``schwab.auth`` in ``sys.modules`` so the lazy import finds the factory.

    Both entries are set, because ``from schwab.auth import client_from_token_file``
    resolves the parent package and then the submodule. ``monkeypatch`` restores the real
    ``sys.modules`` after the test, so a fake never leaks into another one.
    """
    auth = ModuleType("schwab.auth")
    auth.client_from_token_file = factory  # type: ignore[attr-defined]
    package = ModuleType("schwab")
    package.auth = auth  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "schwab", package)
    monkeypatch.setitem(sys.modules, "schwab.auth", auth)


def _write_token(path: Path, creation_timestamp: float) -> None:
    """Write a token file carrying a mint epoch, the field the factory reads back."""
    path.write_text(json.dumps({"creation_timestamp": creation_timestamp}))


def test_from_token_builds_the_client_from_the_token_file(tmp_path, monkeypatch):
    """The factory reads the path it is given and forwards the caller's secrets.

    This covers the seam's contract: the token path reaches the client build, the two
    secrets pass through verbatim, and the enum flag is the ``False`` this layer pins so
    the field groups stay plain strings. The resulting vendor's mint time is the one the
    file names.
    """
    token = tmp_path / "token.json"
    _write_token(token, MINT_BEFORE)
    factory = _RecordingFactory()
    _install_seam(monkeypatch, factory)

    vendor = SchwabVendor.from_token(token, api_key="api-key", app_secret="app-secret")

    assert factory.calls == [(str(token), "api-key", "app-secret", False)]
    assert vendor.token_mint_time() == datetime.fromtimestamp(MINT_BEFORE, tz=UTC)


def test_from_token_reads_the_file_on_every_call_not_once(tmp_path, monkeypatch):
    """A file rewritten between two calls yields a client built from the second contents.

    This is the defect the issue names. A cache inside ``from_token`` would satisfy every
    other test and still keep the daemon on a dead client after a mid-week re-login.
    Keying such a cache on the token path would not help either, because a re-auth
    rewrites the file's contents at an unchanged path.

    Two facts hold the design promise. The factory runs once per call, so the file is
    read again rather than served from a cache. And the second vendor's mint time is the
    rewritten value, so a path-keyed cache that missed the rewrite would fail here too.
    """
    token = tmp_path / "token.json"
    _write_token(token, MINT_BEFORE)
    factory = _RecordingFactory()
    _install_seam(monkeypatch, factory)

    first = SchwabVendor.from_token(token, api_key="api-key", app_secret="app-secret")
    assert first.token_mint_time() == datetime.fromtimestamp(MINT_BEFORE, tz=UTC)

    # A mid-week re-login rewrites the same path with a fresh mint.
    _write_token(token, MINT_AFTER)
    second = SchwabVendor.from_token(token, api_key="api-key", app_secret="app-secret")

    # Fact one: the factory ran a second time, so the file was read again, not cached.
    assert len(factory.calls) == 2
    # Fact two: the second client is built from the rewritten contents.
    assert second.token_mint_time() == datetime.fromtimestamp(MINT_AFTER, tz=UTC)
