"""A full disk under the token file, through the daemon's real cycle entry.

Marketlake #860. On a full root volume the token write raises ``ENOSPC``. Before the fix
that error raised out of the request that refreshed, so every minute lost one request,
usually the quote batch for every ticker, and made one more call to Schwab's token
endpoint, since each cycle's client started from the stale file. Now the refresh writer
keeps the refreshed token in the process and lets the request go on, and later clients run
on it while it is newer than the file, retrying the write each time.

These tests run ``capture.run_cycle_from_config`` over a real ``schwab-py`` client. The
session ``schwab-py`` builds is authlib's ``OAuth2Client``, imported by name into
``schwab.auth``, so replacing that name with one that carries ``httpx.MockTransport``
sends the token request, the chains and the quotes to the fake server below and opens no
socket. ``ENOSPC`` comes from a stand-in for ``lake.reauth.os`` alone, so the journal's own
writes still land. Every token is a made-up string in a temporary file.
"""

from __future__ import annotations

import errno
import functools
import json
import os
import threading
import time
from pathlib import Path

import httpx
import pytest
import schwab.auth
from authlib.integrations.httpx_client import OAuth2Client

from lake import capture, journal
from lake.chain_plan import load_chain_plan
from lake.compact import write_chain_plan
from tests.component.test_cycle_from_config import (
    FIRST_MINUTE,
    ONE_WINDOW,
    WITH_QQQ,
    _chain_body,
    _quote_body,
)
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.full_disk import fill_disk, free_disk

CHAINS = journal.CHAINS_SURFACE
QUOTES = journal.QUOTES_SURFACE
DATA = journal.ROW_KIND_DATA
MINT = 1787529900

EVERY_SURFACE_AS_DATA = {
    (CHAINS, "SPY", DATA, None),
    (CHAINS, "QQQ", DATA, None),
    (QUOTES, "SPY", DATA, None),
    (QUOTES, "QQQ", DATA, None),
}


class _Schwab:
    """Schwab's token endpoint, chains and quotes, counting the token calls."""

    def __init__(self) -> None:
        self.token_calls = 0
        self._lock = threading.Lock()

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/oauth/token":
            with self._lock:
                self.token_calls += 1
                issued = self.token_calls
            return httpx.Response(
                200,
                json={
                    "access_token": f"cycle-access-{issued}",
                    "refresh_token": "cycle-refresh",
                    "token_type": "Bearer",
                    "expires_in": 1800,
                },
            )
        if path == "/marketdata/v1/chains":
            return httpx.Response(200, json=_chain_body(request.url.params["symbol"]))
        if path == "/marketdata/v1/quotes":
            symbols = request.url.params["symbols"].split(",")
            return httpx.Response(200, json=_quote_body(symbols))
        return httpx.Response(404, json={})


def _machine(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **guards: int):
    """A config, roster, plan, lake and expired token on disk, and the server they reach."""
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text(WITH_QQQ)
    plan = tmp_path / "chain_plan.json"
    write_chain_plan(ONE_WINDOW, plan)
    monkeypatch.setattr(capture, "load_chain_plan", lambda: load_chain_plan(plan))
    token = tmp_path / "token.json"
    # A real ``schwab-py`` envelope whose access token lapsed ten seconds ago.
    token.write_text(
        json.dumps(
            {
                "creation_timestamp": MINT,
                "token": {
                    "access_token": "cycle-stale",
                    "refresh_token": "cycle-refresh",
                    "token_type": "Bearer",
                    "expires_at": int(time.time()) - 10,
                },
            }
        )
    )
    config = write_config(tmp_path, lake_root, guards=guards or None)
    server = _Schwab()
    monkeypatch.setattr(
        schwab.auth,
        "OAuth2Client",
        functools.partial(OAuth2Client, transport=httpx.MockTransport(server)),
    )

    def cycle(clock: ManualClock) -> capture.CycleResult:
        return capture.run_cycle_from_config(
            clock=clock, config_path=config, tickers_path=tickers, token_path=token, pid=4242
        )

    return token, server, cycle


def _kinds(result: capture.CycleResult) -> set[tuple[str, str, str, str | None]]:
    return {(seg.surface, seg.ticker, seg.row_kind, seg.error_class) for seg in result.segments}


@pytest.mark.parametrize(
    "guards",
    # The pool submits the quote batch first, so it usually takes the refresh. At a cap of
    # one the first chain window takes it instead, which once lost that window every minute
    # with nothing paged.
    [{}, {"capture_max_concurrency": 1}],
    ids=["pool", "one-at-a-time"],
)
def test_a_full_disk_costs_no_request_and_one_token_call(tmp_path, monkeypatch, capsys, guards):
    token, server, cycle = _machine(tmp_path, monkeypatch, **guards)
    before = token.read_bytes()
    clock = ManualClock(start=FIRST_MINUTE)
    fill_disk(monkeypatch, errno.ENOSPC)

    first = cycle(clock)
    clock.advance(60)
    second = cycle(clock)

    # Both full-disk cycles land every surface, and the second runs on the held token.
    assert _kinds(first) == EVERY_SURFACE_AS_DATA
    assert _kinds(second) == EVERY_SURFACE_AS_DATA
    assert server.token_calls == 1
    assert token.read_bytes() == before
    err = capsys.readouterr().err
    assert err.count("token file write failed") == 1
    assert f"(ENOSPC: {os.strerror(errno.ENOSPC)}) at {token};" in err
    assert "the request goes on" in err

    free_disk(monkeypatch)
    clock.advance(60)
    freed = cycle(clock)

    # The freed cycle makes no token call and lands cycle 0's token in the file.
    assert _kinds(freed) == EVERY_SURFACE_AS_DATA
    assert server.token_calls == 1
    assert json.loads(token.read_text())["token"]["access_token"] == "cycle-access-1"
    err = capsys.readouterr().err
    assert err.count("now holds a token at least as new") == 1
    assert "token file write failed" not in err
    # No token value reaches the log.
    assert "cycle-access" not in err and "cycle-refresh" not in err and "cycle-stale" not in err
