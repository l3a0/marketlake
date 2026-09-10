"""The slice-1 runner orchestration, decided from values alone.

These inject fakes for the two I/O seams and a canned cycle result, then assert the
orchestration rule: on a successful durable cycle the runner backs up and then pings the
health check, in that order, so the one slice-1 ping attests both. A cycle that captured
nothing does neither. A backup that fails blocks the ping and surfaces the error, so the
missed ping catches the single-copy window. Nothing here touches the network or a
subprocess, and no outcome depends on the filesystem, so the tier is unit. The shared
backup fake lists the source it was handed, which is a path these tests never create,
so the listing comes back empty on any machine.
"""

from __future__ import annotations

import io
import urllib.error
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lake import runner
from lake.capture import CycleResult, SegmentError, SegmentOutcome
from lake.journal import ROW_KIND_DATA, ROW_KIND_GAP
from tests.support.backup import FakeBackup
from tests.support.pinger import FakePinger

_SNAP = datetime(2026, 8, 24, 20, 15, tzinfo=UTC)
_URL = "https://hc-ping.com/secret-key/slice1-capture"
_LAKE = Path("/lake")
_TARGET = Path("/ssd/lake")


def _segment(surface: str, ticker: str, row_kind: str) -> SegmentOutcome:
    return SegmentOutcome(
        surface=surface,
        ticker=ticker,
        path=Path(f"/lake/journal/{surface}-{ticker}.arrows"),
        partition=f"journal/{surface}/{ticker}.arrows",
        row_kind=row_kind,
        rows=1,
        error_class=None if row_kind == ROW_KIND_DATA else "http_500",
        fetched_at=_SNAP.isoformat(),
    )


def _data_result() -> CycleResult:
    return CycleResult(
        snap_ts=_SNAP,
        segments=(
            _segment("chains", "SPY", ROW_KIND_DATA),
            _segment("quotes", "SPY", ROW_KIND_DATA),
        ),
    )


def _all_gap_result() -> CycleResult:
    return CycleResult(
        snap_ts=_SNAP,
        segments=(
            _segment("chains", "SPY", ROW_KIND_GAP),
            _segment("quotes", "SPY", ROW_KIND_GAP),
        ),
    )


def _run(cycle_runner):
    events: list[str] = []
    pinger = FakePinger(events)
    backup = FakeBackup(events)
    outcome = runner.run_once(
        cycle_runner,
        pinger=pinger,
        ping_url=_URL,
        backup=backup,
        lake_root=_LAKE,
        backup_target=_TARGET,
    )
    return outcome, pinger, backup, events


def test_successful_cycle_backs_up_then_pings():
    outcome, pinger, backup, events = _run(_data_result)
    assert outcome.succeeded is True
    assert outcome.backed_up is True and outcome.pinged is True
    # The backup targets the SSD, and the ping fires on the configured URL.
    assert backup.calls == [(_LAKE, _TARGET)]
    assert pinger.urls == [_URL]
    # Order matters: rsync first, then the ping, so the one ping attests both.
    assert events == ["backup", "ping"]


def test_a_failed_ping_is_named_and_the_run_keeps_its_verdict():
    # The ping is the last step, after the capture is durable and the backup is done.
    # A raise there used to propagate before main printed the run's summary, turning a
    # successful capture into a traceback and a non-zero exit. The ping is lost either
    # way, and healthchecks pages for it after the grace. The verdict is not.
    events: list[str] = []

    class Boom:
        def ping(self, url: str) -> None:
            events.append("ping")
            raise urllib.error.URLError(OSError("connection refused"))

    outcome = runner.run_once(
        _data_result,
        pinger=Boom(),
        ping_url=_URL,
        backup=FakeBackup(events),
        lake_root=_LAKE,
        backup_target=_TARGET,
    )
    assert outcome.succeeded is True
    assert outcome.backed_up is True
    assert outcome.pinged is False
    assert outcome.problem == "ping failed: URLError"
    # The backup still ran and its order is still pinned.
    assert events == ["backup", "ping"]


def test_a_failed_ping_never_carries_the_key():
    class Boom:
        def ping(self, url: str) -> None:
            raise urllib.error.HTTPError(_URL, 500, "Server Error", {}, io.BytesIO(b""))

    outcome = runner.run_once(
        _data_result,
        pinger=Boom(),
        ping_url=_URL,
        backup=FakeBackup([]),
        lake_root=_LAKE,
        backup_target=_TARGET,
    )
    # Equality is the stronger claim. It says what the line is, so no part of the URL
    # can be in it.
    assert outcome.problem == "ping failed: HTTPError"


def test_cycle_that_captured_nothing_does_not_ping_or_back_up():
    outcome, pinger, backup, events = _run(_all_gap_result)
    assert outcome.succeeded is False
    assert outcome.pinged is False and outcome.backed_up is False
    assert pinger.urls == []
    assert backup.calls == []
    assert events == []


def test_a_write_error_blocks_the_ping():
    def cycle_runner() -> CycleResult:
        return CycleResult(
            snap_ts=_SNAP,
            segments=(_segment("chains", "SPY", ROW_KIND_DATA),),
            errors=(SegmentError("quotes", "SPY", "disk_error"),),
        )

    outcome, pinger, backup, events = _run(cycle_runner)
    assert outcome.succeeded is False
    assert events == []


def test_a_raising_cycle_never_pings():
    def cycle_runner() -> CycleResult:
        raise RuntimeError("config exploded")

    events: list[str] = []
    pinger = FakePinger(events)
    backup = FakeBackup(events)
    with pytest.raises(RuntimeError):
        runner.run_once(
            cycle_runner,
            pinger=pinger,
            ping_url=_URL,
            backup=backup,
            lake_root=_LAKE,
            backup_target=_TARGET,
        )
    assert events == []


def test_a_failing_backup_blocks_the_ping_and_surfaces():
    # A successful cycle, but the backup raises. The ping must not fire, and the error
    # must propagate, so the missed ping makes the dead-man catch the single-copy window.
    events: list[str] = []
    pinger = FakePinger(events)

    class RaisingBackup:
        def sync(self, source: Path, target: Path) -> None:
            events.append("backup-attempt")
            raise runner.BackupTargetUnavailable("ssd unplugged")

    with pytest.raises(runner.BackupTargetUnavailable):
        runner.run_once(
            _data_result,
            pinger=pinger,
            ping_url=_URL,
            backup=RaisingBackup(),
            lake_root=_LAKE,
            backup_target=_TARGET,
        )
    # The backup was attempted; the ping never fired.
    assert events == ["backup-attempt"]
    assert pinger.urls == []


def test_cycle_succeeded_predicate():
    assert runner.cycle_succeeded(_data_result()) is True
    assert runner.cycle_succeeded(_all_gap_result()) is False


def test_main_wires_the_live_seams(tmp_path, monkeypatch, capsys):
    # The runner half of the rule the daemon's `main` test holds. `run_once_from_config`
    # no longer defaults its seams, so `main` is the caller that must supply the real
    # pair. Nothing exercised this entry before, so neither half of the rule was held.
    from types import SimpleNamespace

    from lake.runner import RsyncBackup, UrllibPinger
    from tests.support.config import write_config

    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)

    seen: dict = {}

    def stub(**kwargs):
        seen.update(kwargs)
        return SimpleNamespace(
            result=CycleResult(datetime(2026, 9, 2, 16, 0, tzinfo=UTC), ()),
            succeeded=True,
            pinged=True,
            backed_up=True,
            problem=None,
        )

    monkeypatch.setattr(runner, "run_once_from_config", stub)
    assert runner.main(["run", "--config", str(config)]) == 0

    assert isinstance(seen["pinger"], UrllibPinger)
    assert isinstance(seen["backup"], RsyncBackup)
    # The ping key never reaches stdout, only the slug.
    assert "secret-key" not in capsys.readouterr().out
