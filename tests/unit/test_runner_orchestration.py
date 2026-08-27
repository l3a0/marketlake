"""The slice-1 runner orchestration, decided from values alone.

These inject fakes for the two I/O seams and a canned cycle result, then assert the
orchestration rule: on a successful durable cycle the runner pings the health check and
then backs up, in that order; a cycle that captured nothing does neither. Nothing here
touches the network, a subprocess, or the filesystem, so the tier is unit.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from lake import runner
from lake.capture import CycleResult, SegmentError, SegmentOutcome
from lake.journal import ROW_KIND_DATA, ROW_KIND_GAP

_SNAP = datetime(2026, 8, 24, 20, 15, tzinfo=UTC)
_URL = "https://hc-ping.com/secret-key/slice1-capture"
_LAKE = Path("/lake")
_TARGET = Path("/ssd/lake")


class FakePinger:
    """Records the URL it was asked to ping, appending to a shared event log."""

    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.urls: list[str] = []

    def ping(self, url: str) -> None:
        self.urls.append(url)
        self.events.append("ping")


class FakeBackup:
    """Records each sync, appending to a shared event log."""

    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.calls: list[tuple[Path, Path]] = []

    def sync(self, source: Path, target: Path) -> None:
        self.calls.append((source, target))
        self.events.append("backup")


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


def test_successful_cycle_pings_then_backs_up():
    outcome, pinger, backup, events = _run(_data_result)
    assert outcome.succeeded is True
    assert outcome.pinged is True and outcome.backed_up is True
    # The ping fires on the configured URL, and the backup targets the SSD.
    assert pinger.urls == [_URL]
    assert backup.calls == [(_LAKE, _TARGET)]
    # Order matters: ping first, then rsync.
    assert events == ["ping", "backup"]


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


def test_cycle_succeeded_predicate():
    assert runner.cycle_succeeded(_data_result()) is True
    assert runner.cycle_succeeded(_all_gap_result()) is False
