"""The daemon's journal metadata stamp, wired through the production entry.

The Now panel reads the token's mint time, the roster, and the last dead-man ping from
under ``lake_root``, because the dashboard never opens ``~/.config``. Two writers put
them there. A capture cycle stamps the mint off the vendor it fetched with, which
``tests/component/test_capture_cycle.py`` covers. Everything else is here: the minutes off
the capture window, where no cycle runs and no client exists.

These drive ``run_loop_from_config`` over a real lake, a real token file, and a real
config, with the clock, the calendar, the cycle, the pinger, the transport, and the
compaction spawn all faked. So the tier is component. Deleting either binding in the
daemon must not leave the suite green, which is what each test is written to catch.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path

from lake import daemon
from lake.capture import CycleResult
from lake.metadata import read_metadata
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.pinger import FakePinger

WEEK = date(2026, 8, 31)  # a Monday
NEXT_WEEK = date(2026, 9, 7)
MINTED = et(2026, 8, 30, 20, 5)  # the Sunday re-auth before that week


class Broken:
    """A transport that cannot deliver, so no page leaves a test."""

    def send(self, message) -> None:
        raise OSError("no network")


def _no_cycle(*, close_tag: str | None, session_phase: str | None) -> CycleResult:
    """A cycle runner for minutes the loop must never capture in."""
    raise AssertionError("no cycle should run in these minutes")


def _token(tmp_path: Path, minted: datetime = MINTED) -> Path:
    """A ``schwab-py``-shaped token file. Only ``creation_timestamp`` is ever read."""
    path = tmp_path / "token.json"
    path.write_text(
        json.dumps(
            {
                "creation_timestamp": minted.timestamp(),
                "token": {"access_token": "SECRET", "refresh_token": "ALSO-SECRET"},
            }
        )
    )
    return path


def _run(
    tmp_path: Path,
    *,
    start: datetime,
    ticks: int = 1,
    token: Path | None = None,
    pinger: FakePinger | None = None,
    roster: str | None = None,
) -> Path:
    """Run the loop for a few ticks over a throwaway lake, and return the lake root."""
    lake_root = tmp_path / "lake"
    lake_root.mkdir(exist_ok=True)
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text(
        roster
        if roster is not None
        else "SPY: {options: true, chain_cadence: 1m}\nXYZ: {options: false}\n"
    )
    counted = [0]

    def more() -> bool:
        counted[0] += 1
        return counted[0] <= ticks

    daemon.run_loop_from_config(
        config_path=str(config),
        tickers_path=str(tickers),
        token_path=str(_token(tmp_path) if token is None else token),
        clock=ManualClock(start=start),
        calendar=weekday_sessions(WEEK, NEXT_WEEK),
        assertion_runner=lambda args: None,
        transport=Broken(),
        pinger=pinger if pinger is not None else FakePinger(),
        compaction_runner=lambda args: None,
        cycle_runner=_no_cycle,
        should_continue=more,
    )
    return lake_root


def test_an_idle_minute_stamps_the_mint_time_and_the_roster(tmp_path):
    # Before the open on a weekday. No cycle runs, so the stamp is the only thing that
    # can carry the token's age and the ticker list to the panel.
    lake_root = _run(tmp_path, start=et(2026, 8, 31, 8, 40))

    stamp = read_metadata(lake_root)
    assert stamp.token_minted_at == MINTED
    assert stamp.stamped_at == et(2026, 8, 31, 8, 41)
    assert stamp.tickers == {"SPY": ("chains", "quotes"), "XYZ": ("quotes",)}


def test_an_idle_stamp_omits_a_ticker_disabled_in_place(tmp_path):
    # A disabled ticker still names an entry in tickers.yaml, but an idle stamp must not
    # carry it into the dashboard's ticker list, or the panel would show a ticker not
    # actually being captured until the next live cycle overwrites the stamp.
    lake_root = _run(
        tmp_path,
        start=et(2026, 8, 31, 8, 40),
        roster="SPY: {options: true, chain_cadence: 1m}\nXYZ: {options: false, enabled: false}\n",
    )

    stamp = read_metadata(lake_root)
    assert stamp.tickers == {"SPY": ("chains", "quotes")}


def test_the_sunday_re_auth_reaches_the_panel_the_same_night(tmp_path):
    # The design's own case. The machine is awake for the canary window, the ritual mints
    # a token, and the panel is meant to show that mint on Sunday rather than on Monday.
    # Sunday captures nothing, so an idle stamp is the only path.
    minted = et(2026, 9, 6, 20, 30)
    lake_root = _run(
        tmp_path,
        start=et(2026, 9, 6, 20, 45),
        token=_token(tmp_path, minted),
    )

    assert read_metadata(lake_root).token_minted_at == minted


def test_the_stamp_never_carries_token_material(tmp_path):
    # The stamp is a fact about a secret file. The file itself holds a token, and the
    # whole lake is read by a service and synced to a backup disk.
    lake_root = _run(tmp_path, start=et(2026, 8, 31, 8, 40))

    written = (lake_root / "journal" / "metadata.json").read_text()
    assert "SECRET" not in written
    assert "access_token" not in written


def test_a_capture_minute_is_left_to_the_cycle_s_own_stamp(tmp_path):
    # Inside the capture window the cycle stamps the mint off the vendor it fetched
    # with, which is the design's rule. A second stamp from the token file here would
    # report a token capture is not using.
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("SPY: {options: true, chain_cadence: 1m}\n")
    ran: list[datetime] = []
    counted = [0]

    def once() -> bool:
        counted[0] += 1
        return counted[0] <= 1

    def record_cycle(*, close_tag: str | None, session_phase: str | None) -> CycleResult:
        slot = et(2026, 8, 31, 12, 1)
        ran.append(slot)
        return CycleResult(slot, ())

    daemon.run_loop_from_config(
        config_path=str(config),
        tickers_path=str(tickers),
        token_path=str(_token(tmp_path)),
        clock=ManualClock(start=et(2026, 8, 31, 12, 0)),
        calendar=weekday_sessions(WEEK),
        assertion_runner=lambda args: None,
        transport=Broken(),
        pinger=FakePinger(),
        compaction_runner=lambda args: None,
        cycle_runner=record_cycle,
        should_continue=once,
    )

    assert ran, "the loop never reached the capture window"
    assert read_metadata(lake_root).token_minted_at is None


def test_a_landed_dead_man_ping_is_written_where_the_panel_reads_it(tmp_path):
    pinger = FakePinger()
    lake_root = _run(tmp_path, start=et(2026, 8, 31, 8, 40), pinger=pinger)

    assert pinger.urls, "the idle minute sent no ping"
    assert read_metadata(lake_root).dead_man_last_ping == et(2026, 8, 31, 8, 41)


def test_a_missing_token_file_costs_the_stamp_and_not_the_loop(tmp_path):
    # Mid-re-auth the file is briefly gone. The daemon keeps ticking and the panel keeps
    # showing the last mint it knew, which for a fresh lake is nothing at all.
    lake_root = _run(tmp_path, start=et(2026, 8, 31, 8, 40), token=tmp_path / "absent.json")

    stamp = read_metadata(lake_root)
    assert stamp.token_minted_at is None
    assert stamp.tickers == {}
    # The dead-man still ran, so the tick itself completed.
    assert stamp.dead_man_last_ping == et(2026, 8, 31, 8, 41)


def test_a_later_tick_replaces_the_earlier_stamp(tmp_path):
    lake_root = _run(tmp_path, start=et(2026, 8, 31, 8, 40), ticks=3)

    # Three ticks, and the stamp carries the last of them rather than the first.
    assert read_metadata(lake_root).stamped_at == et(2026, 8, 31, 8, 41) + timedelta(minutes=2)
