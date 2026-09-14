"""A healthy weekday morning, end to end, with nothing about it faked but the clock.

The daemon runs its loop over a real lake and spawns a caffeinate through a runner that
hands back a real pid. The self-check then runs the real parser over a pmset dump shaped
like the machine's, naming that pid. It must ping.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

from lake import control_plane as cp
from lake import daemon
from lake.capture import CycleResult
from lake.metadata import read_metadata
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.pinger import FakePinger

WEEK = date(2026, 8, 31)
MINTED = et(2026, 8, 30, 20, 5)


class Broken:
    def send(self, message) -> None:
        raise OSError("no network")


class RealisticChild:
    def __init__(self, pid: int) -> None:
        self.pid = pid

    def poll(self) -> int | None:
        return None


def _dump(pid: int) -> str:
    """The shape a live machine prints, continuation lines and all."""
    return f"""Assertion status system-wide:
   PreventUserIdleDisplaySleep    1
   PreventUserIdleSystemSleep     1
   PreventSystemSleep             0
Listed by owning process:
   pid 400(powerd): [0x0007cb61] 01:27:18 PreventUserIdleSystemSleep named: "Powerd"
   pid {pid}(caffeinate): [0x0007dfec] 00:00:01 PreventUserIdleSystemSleep named: "caff"
\tDetails: caffeinate asserting for 36900 secs
\tTimeout will fire in 36899 secs Action=TimeoutActionRelease
"""


def _run_daemon(tmp_path: Path, start: datetime, pid: int) -> Path:
    lake_root = tmp_path / "lake"
    lake_root.mkdir(exist_ok=True)
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("SPY: {options: true, chain_cadence: 1m}\n")
    token = tmp_path / "token.json"
    token.write_text(json.dumps({"creation_timestamp": MINTED.timestamp(), "token": {}}))
    counted = [0]

    def once() -> bool:
        counted[0] += 1
        return counted[0] <= 1

    def no_cycle(*, close_tag, session_phase) -> CycleResult:
        raise AssertionError("no cycle here")

    daemon.run_loop_from_config(
        config_path=str(config),
        tickers_path=str(tickers),
        token_path=str(token),
        clock=ManualClock(start=start),
        calendar=weekday_sessions(WEEK),
        assertion_runner=lambda args: RealisticChild(pid),
        transport=Broken(),
        pinger=FakePinger(),
        compaction_runner=lambda args: None,
        cycle_runner=no_cycle,
        should_continue=once,
    )
    return lake_root


def test_a_healthy_weekday_morning_still_pings(tmp_path):
    lake_root = _run_daemon(tmp_path, et(2026, 8, 31, 8, 25), pid=61234)
    stamped = read_metadata(lake_root).assertion_pid
    assert stamped == 61234, "the daemon stamped no pid on a healthy morning"

    pinger = FakePinger()
    outcome = cp.self_check(
        probe=lambda label: True,
        pinger=pinger,
        ping_url="https://example.invalid/ping",
        # The real parser over a realistic dump. Only the dump is injected.
        assertion_probe=lambda pid: cp.parse_pmset_assertions(_dump(61234), pid=pid),
        assertion_pid=stamped,
        now=et(2026, 8, 31, 8, 30),
    )

    assert outcome.problem is None, outcome.problem
    assert outcome.assertion_held is True
    assert outcome.pinged is True, "a healthy morning stopped pinging"
    assert pinger.urls == ["https://example.invalid/ping"]


def test_a_healthy_sunday_evening_still_pings(tmp_path):
    # The Sunday window runs 19:55 to 23:30, and the canary job checks inside it.
    lake_root = _run_daemon(tmp_path, et(2026, 9, 6, 20, 0), pid=7788)
    stamped = read_metadata(lake_root).assertion_pid
    assert stamped == 7788, "the Sunday window stamped no pid"

    outcome = cp.self_check(
        probe=lambda label: True,
        pinger=FakePinger(),
        ping_url="https://example.invalid/ping",
        assertion_probe=lambda pid: cp.parse_pmset_assertions(_dump(7788), pid=pid),
        assertion_pid=stamped,
        now=et(2026, 9, 6, 20, 30),
    )

    assert outcome.pinged is True and outcome.problem is None


def test_the_hand_run_caffeinate_from_last_night_is_refused(tmp_path):
    """The bug #111 names. Same healthy-looking dump, a pid the daemon never spawned."""
    lake_root = _run_daemon(tmp_path, et(2026, 8, 31, 8, 25), pid=61234)
    stamped = read_metadata(lake_root).assertion_pid

    pinger = FakePinger()
    outcome = cp.self_check(
        probe=lambda label: True,
        pinger=pinger,
        ping_url="https://example.invalid/ping",
        # A real assertion, really held, by a process the daemon did not spawn.
        assertion_probe=lambda pid: cp.parse_pmset_assertions(_dump(99999), pid=pid),
        assertion_pid=stamped,
        now=et(2026, 8, 31, 8, 30),
    )

    assert outcome.pinged is False, "a foreign caffeinate still satisfied the check"
    assert outcome.assertion_held is False
    assert pinger.urls == []
