"""The daemon's journal metadata stamp, wired through the production entry.

The Now panel reads the token's mint time, the roster, and the last dead-man ping from
under ``lake_root``, because the dashboard never opens ``~/.config``. The 08:30
self-check reads a fourth fact from the same file, the pid of the ``caffeinate`` the
daemon is holding, because it runs in its own process and cannot see the daemon's handle
on that child. Two writers put the panel's three there. A capture cycle stamps the mint
off the vendor it fetched with, which ``tests/component/test_capture_cycle.py`` covers.
Everything else is here: the minutes off the capture window, where no cycle runs and no
client exists, and the pid, which is stamped on every minute of an open window.

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
from lake.metadata import read_metadata, stamp_assertion_pid
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


class PidChild:
    """A spawned child that reports a pid and stays alive, the way ``Popen`` does."""

    def __init__(self, pid: int) -> None:
        self.pid = pid

    def poll(self) -> int | None:
        return None


class PidRunner:
    """Hands back a fresh ``PidChild`` per spawn, each with its own pid."""

    def __init__(self, *pids: int) -> None:
        self._pids = list(pids)
        self.children: list[PidChild] = []
        self.spawns = 0

    def __call__(self, args) -> object:
        pid = self._pids[min(self.spawns, len(self._pids) - 1)]
        self.spawns += 1
        child = PidChild(pid)
        self.children.append(child)
        return child


def _run(
    tmp_path: Path,
    *,
    start: datetime,
    ticks: int = 1,
    token: Path | None = None,
    pinger: FakePinger | None = None,
    roster: str | None = None,
    assertion_runner=lambda args: None,
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
        assertion_runner=assertion_runner,
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


# -- the caffeinate pid the 08:30 self-check matches against ----------------------------


def test_a_tick_inside_the_window_stamps_the_caffeinate_it_holds(tmp_path):
    """The stamp is the only way the check learns which ``caffeinate`` is the daemon's.

    Without it the check matches any ``caffeinate`` holding idle sleep off, so a hand-run
    one left from the night before reads as the daemon's own and the machine sleeps when
    its timer runs out.
    """
    runner = PidRunner(4242)
    lake_root = _run(tmp_path, start=et(2026, 8, 31, 8, 40), assertion_runner=runner)

    assert runner.spawns == 1, "the tick never spawned a caffeinate to stamp"
    assert read_metadata(lake_root).assertion_pid == 4242


def test_a_capture_minute_stamps_the_pid_too(tmp_path):
    """The assertion window brackets the capture window, so the pid cannot skip it.

    The idle mint stamp does skip capture minutes, because the cycle stamps the mint
    itself. Nothing stamps the pid but this hook, and a check run at 12:00 for a holiday
    compaction or a restart would find no pid if it stood down inside the session.
    """
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("SPY: {options: true, chain_cadence: 1m}\n")
    runner = PidRunner(4242)
    counted = [0]

    def once() -> bool:
        counted[0] += 1
        return counted[0] <= 1

    def record_cycle(*, close_tag: str | None, session_phase: str | None) -> CycleResult:
        return CycleResult(et(2026, 8, 31, 12, 1), ())

    daemon.run_loop_from_config(
        config_path=str(config),
        tickers_path=str(tickers),
        token_path=str(_token(tmp_path)),
        clock=ManualClock(start=et(2026, 8, 31, 12, 0)),
        calendar=weekday_sessions(WEEK),
        assertion_runner=runner,
        transport=Broken(),
        pinger=FakePinger(),
        compaction_runner=lambda args: None,
        cycle_runner=record_cycle,
        should_continue=once,
    )

    assert read_metadata(lake_root).assertion_pid == 4242


def test_a_tick_outside_any_window_clears_the_stamped_pid(tmp_path):
    """A pid left standing past its window names a process that has already exited.

    ``caffeinate -i -t`` releases itself when the window ends. Carrying the number
    forward would make the stamp a claim about a dead process, and the one reader it has
    would be told either a lie or, once the pid is reused, something worse.
    """
    runner = PidRunner(4242)
    # 18:50 on a weekday, five minutes past the window's 18:45 end.
    lake_root = _run(tmp_path, start=et(2026, 8, 31, 18, 50), assertion_runner=runner)

    assert runner.spawns == 0, "a caffeinate was spawned outside the window"
    assert read_metadata(lake_root).assertion_pid is None


def test_a_restart_stamps_its_own_child_over_the_dead_one(tmp_path):
    """A daemon that restarts mid-window spawns a new ``caffeinate`` with a new pid.

    Stamping once at startup, or only on the first hold of a lake's life, would leave the
    predecessor's pid standing. The check would then match nothing and page on a machine
    that is being held awake perfectly well.
    """
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    _run(
        tmp_path,
        start=et(2026, 8, 31, 8, 40),
        assertion_runner=PidRunner(4242),
    )
    assert read_metadata(lake_root).assertion_pid == 4242

    _run(
        tmp_path,
        start=et(2026, 8, 31, 9, 10),
        assertion_runner=PidRunner(5353),
    )

    assert read_metadata(lake_root).assertion_pid == 5353


def test_a_re_take_inside_the_window_stamps_the_replacement(tmp_path):
    """The holder re-spawns on a child that has gone, and the stamp has to follow it.

    A stamp written once per window would name the killed process for the rest of the
    day. The machine is held awake by the replacement, and the check would page anyway.
    """
    runner = PidRunner(4242, 5353)
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("SPY: {options: true, chain_cadence: 1m}\n")
    counted = [0]
    seen: list[int | None] = []

    def three() -> bool:
        counted[0] += 1
        return counted[0] <= 3

    def kill_the_first(slot: datetime) -> None:
        # Read after each tick, so the sequence of stamps is what the test reads rather
        # than only the last one. The first child dies between tick one and tick two.
        seen.append(read_metadata(lake_root).assertion_pid)
        if len(seen) == 1:
            runner.children[0].poll = lambda: 0

    daemon.run_loop_from_config(
        config_path=str(config),
        tickers_path=str(tickers),
        token_path=str(_token(tmp_path)),
        hooks=daemon.DaemonHooks(on_tick=kill_the_first),
        clock=ManualClock(start=et(2026, 8, 31, 8, 40)),
        calendar=weekday_sessions(WEEK),
        assertion_runner=runner,
        transport=Broken(),
        pinger=FakePinger(),
        compaction_runner=lambda args: None,
        cycle_runner=_no_cycle,
        should_continue=three,
    )

    assert runner.spawns == 2, "the dead child was never replaced"
    assert seen[0] == 4242
    assert seen[1:] == [5353, 5353], f"the stamp did not follow the re-take: {seen}"


def test_a_spawn_that_fails_leaves_no_pid_stamped(tmp_path):
    """Nothing is holding the machine awake, and the stamp must not claim otherwise.

    The holder swallows the failure so the daemon keeps ticking. If the stamp carried a
    pid anyway, the one check that could still page while the machine is awake to send it
    would go quiet.
    """

    def refuse(args):
        raise OSError("no process table")

    lake_root = _run(tmp_path, start=et(2026, 8, 31, 8, 40), assertion_runner=refuse)

    assert read_metadata(lake_root).assertion_pid is None


def test_a_runner_that_hands_back_no_pid_stamps_nothing(tmp_path):
    """The handle protocol does not require a pid, and that answer travels honestly.

    A caller whose runner returns something unidentifiable gets a check that says it
    could not confirm, rather than a pid invented to fill the field.
    """
    lake_root = _run(tmp_path, start=et(2026, 8, 31, 8, 40), assertion_runner=lambda args: None)

    assert read_metadata(lake_root).assertion_pid is None


def test_the_pid_stamp_leaves_the_panel_s_own_keys_alone(tmp_path):
    """Three writers share one file, and each may only replace its own keys.

    The pid rides a hook that fires every minute, including the minutes the mint stamp
    writes in. A merge that dropped the other keys would blank the Now panel's token age
    and ticker list on every tick.
    """
    lake_root = _run(
        tmp_path,
        start=et(2026, 8, 31, 8, 40),
        ticks=3,
        assertion_runner=PidRunner(4242),
    )

    stamp = read_metadata(lake_root)
    assert stamp.assertion_pid == 4242
    assert stamp.token_minted_at == MINTED
    assert stamp.tickers == {"SPY": ("chains", "quotes"), "XYZ": ("quotes",)}
    assert stamp.dead_man_last_ping is not None


def test_a_stamp_that_raises_anything_at_all_costs_a_minute_and_not_the_daemon(
    tmp_path, monkeypatch
):
    """The hook is not wrapped by ``run_loop``, so anything escaping it exits the process.

    A full disk and a read-only lake root raise ``OSError``, and the guard is broad rather
    than betting that list is complete. KeepAlive would relaunch into the same tick, which
    is the crash loop this whole area exists to prevent.

    The raise is injected rather than built out of a corrupt file on disk. A file-shaped
    version would have to pick a nesting depth, and the depth at which JSON gives up moves
    with how much recursion the caller has already spent: the same payload decodes in a
    bare script and raises under pytest. A test that picked one would pass or fail by
    where it was called from.
    """

    def explode(lake_root_arg, *, pid):
        raise RecursionError("maximum recursion depth exceeded")

    monkeypatch.setattr(daemon, "stamp_assertion_pid", explode)

    # The loop completing at all is the assertion: a raise from the hook never returns.
    _run(tmp_path, start=et(2026, 8, 31, 8, 40), assertion_runner=PidRunner(4242))


def test_a_stamp_deleted_mid_window_comes_back_on_the_next_tick(tmp_path):
    """The daemon remembers nothing about what it stamped, so the file decides.

    A guard held in memory would read "already stamped" for the rest of the window and
    leave the 08:30 check with no pid to match on a machine being held awake perfectly
    well.
    """
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("SPY: {options: true, chain_cadence: 1m}\n")
    counted = [0]
    seen: list[int | None] = []

    def three() -> bool:
        counted[0] += 1
        return counted[0] <= 3

    def delete_after_the_first(slot: datetime) -> None:
        seen.append(read_metadata(lake_root).assertion_pid)
        if len(seen) == 1:
            (lake_root / "journal" / "metadata.json").unlink()

    daemon.run_loop_from_config(
        config_path=str(config),
        tickers_path=str(tickers),
        token_path=str(_token(tmp_path)),
        hooks=daemon.DaemonHooks(on_tick=delete_after_the_first),
        clock=ManualClock(start=et(2026, 8, 31, 8, 40)),
        calendar=weekday_sessions(WEEK),
        assertion_runner=PidRunner(4242),
        transport=Broken(),
        pinger=FakePinger(),
        compaction_runner=lambda args: None,
        cycle_runner=_no_cycle,
        should_continue=three,
    )

    assert seen[0] == 4242
    assert read_metadata(lake_root).assertion_pid == 4242, f"the pid never came back: {seen}"


def test_a_fresh_daemon_clears_a_predecessor_s_pid_before_any_window_opens(tmp_path):
    """The stamp says what is held now, and a new daemon holding nothing must say so.

    A daemon killed mid-window leaves its pid behind. Its successor starting in the
    evening holds nothing, and a stamp that still named the dead process would be a claim
    about a pid the operating system is free to hand to something else overnight.
    """
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    stamp_assertion_pid(lake_root, pid=4242)

    # 18:50, past the window's 18:45 end, so the new daemon spawns nothing.
    _run(tmp_path, start=et(2026, 8, 31, 18, 50), assertion_runner=PidRunner(5353))

    assert read_metadata(lake_root).assertion_pid is None, "a dead predecessor's pid stood"


def test_a_stamp_the_daemon_could_not_write_is_written_on_the_next_tick(tmp_path, monkeypatch):
    """The swallow is only half a promise. The other half is that the next minute retries.

    A write that failed at the minute a window opened would otherwise leave the pid
    unstamped for the rest of that window, and the 08:30 check pages a machine that is
    genuinely being held awake. Nothing here remembers that a tick was skipped, which is
    what makes the retry structural rather than a thing to get right.
    """
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("SPY: {options: true, chain_cadence: 1m}\n")
    real = daemon.stamp_assertion_pid
    calls = [0]

    def fail_the_first(lake_root_arg, *, pid):
        calls[0] += 1
        if calls[0] == 1:
            raise OSError("no space left on device")
        real(lake_root_arg, pid=pid)

    monkeypatch.setattr(daemon, "stamp_assertion_pid", fail_the_first)
    counted = [0]

    def twice() -> bool:
        counted[0] += 1
        return counted[0] <= 2

    daemon.run_loop_from_config(
        config_path=str(config),
        tickers_path=str(tickers),
        token_path=str(_token(tmp_path)),
        clock=ManualClock(start=et(2026, 8, 31, 8, 40)),
        calendar=weekday_sessions(WEEK),
        assertion_runner=PidRunner(4242),
        transport=Broken(),
        pinger=FakePinger(),
        compaction_runner=lambda args: None,
        cycle_runner=_no_cycle,
        should_continue=twice,
    )

    assert calls[0] == 2, "the tick after a failed write did not try again"
    assert read_metadata(lake_root).assertion_pid == 4242, "the pid never landed"
