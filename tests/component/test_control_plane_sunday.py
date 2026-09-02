"""The Sunday maintenance job across one real boundary: the filesystem.

The scrub reads a lake the fixture builder put on disk. Everything else is injected:
``now``, the calendar, the schedule reader, the canary, and the pinger. The rule under
test is that the ping fires only when every check passes, and that every failure is
named at once.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

from lake import control_plane as cp
from lake.calendar import MARKET_TZ
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.lake import FixtureLake


def _et(*args: int) -> datetime:
    return datetime(*args, tzinfo=MARKET_TZ)


def _week(monday: date) -> FakeCalendar:
    table = {}
    for offset in range(5):
        day = monday + timedelta(days=offset)
        table[day] = SessionTimes(
            open=_et(day.year, day.month, day.day, 9, 30),
            close=_et(day.year, day.month, day.day, 16, 0),
        )
    return FakeCalendar(table)


CALENDAR = _week(date(2026, 8, 31))
SUNDAY_20 = _et(2026, 8, 30, 20, 0)
SUNDAY_19 = _et(2026, 8, 30, 19, 0)
URL = "https://hc-ping.com/secret-key/sunday"

REPEAT_ONLY = "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n"
BOTH = REPEAT_ONLY + "Scheduled power events:\n [0]  wakepoweron at 08/30/26 19:55:00 by 'pmset'\n"


class FakePinger:
    def __init__(self) -> None:
        self.urls: list[str] = []

    def ping(self, url: str) -> None:
        self.urls.append(url)


def _clean_lake(fixture_lake: FixtureLake) -> Path:
    fixture_lake.with_chains("SPY", date(2026, 8, 28))
    fixture_lake.with_quotes("SPY", date(2026, 8, 28))
    return fixture_lake.build()


def _run(lake_root: Path, *, now=SUNDAY_20, schedule=REPEAT_ONLY, canary=None, mint=None):
    pinger = FakePinger()
    kwargs = {}
    if canary is not None:
        kwargs["canary"] = canary
    outcome = cp.sunday_maintenance(
        lake_root=lake_root,
        now=now,
        calendar=CALENDAR,
        schedule_reader=lambda: schedule,
        pinger=pinger,
        ping_url=URL,
        mint=mint,
        **kwargs,
    )
    return outcome, pinger


def test_clean_scrub_and_the_repeat_alarm_ping_at_the_maintenance_time(fixture_lake):
    # At Sunday 20:00 the one-shot has fired and left the schedule. Only the repeat
    # alarm is expected, per the design's read-back caveat.
    outcome, pinger = _run(_clean_lake(fixture_lake))
    assert outcome.scrub.ok and outcome.alarms.ok and outcome.canary_passed
    assert outcome.covered is None
    assert outcome.pinged is True
    assert outcome.problems == ()
    assert pinger.urls == [URL]


def test_before_the_wake_both_alarms_are_required(fixture_lake):
    root = _clean_lake(fixture_lake)
    missing, pinger = _run(root, now=SUNDAY_19, schedule=REPEAT_ONLY)
    assert missing.pinged is False and pinger.urls == []
    assert any("one-shot" in p for p in missing.problems)
    present, pinger = _run(root, now=SUNDAY_19, schedule=BOTH)
    assert present.pinged is True and pinger.urls == [URL]


def test_a_failed_scrub_blocks_the_ping(fixture_lake):
    root = _clean_lake(fixture_lake)
    # Corrupt a manifested partition so the forward pass sees a sha mismatch.
    fixture_lake.partition_path("chains", "SPY", date(2026, 8, 28)).write_bytes(b"torn")
    outcome, pinger = _run(root)
    assert outcome.scrub.ok is False
    assert outcome.pinged is False
    assert pinger.urls == []
    assert any(p.startswith("scrub failed") for p in outcome.problems)


def test_a_missing_lake_root_is_a_failure_not_a_clean_scrub(tmp_path):
    outcome, pinger = _run(tmp_path / "nowhere")
    assert outcome.pinged is False
    assert pinger.urls == []
    assert any("lake root missing" in p for p in outcome.problems)


def test_a_missing_repeat_alarm_blocks_the_ping(fixture_lake):
    outcome, pinger = _run(_clean_lake(fixture_lake), schedule="No scheduled events.\n")
    assert outcome.alarms.repeat_ok is False
    assert outcome.pinged is False
    assert pinger.urls == []


def test_a_failing_canary_blocks_the_ping(fixture_lake):
    outcome, pinger = _run(_clean_lake(fixture_lake), canary=lambda: False)
    assert outcome.canary_passed is False
    assert outcome.pinged is False
    assert pinger.urls == []
    assert "canary call failed" in outcome.problems


def test_a_stale_mint_blocks_the_ping_and_a_fresh_one_passes(fixture_lake):
    root = _clean_lake(fixture_lake)
    stale, pinger = _run(root, mint=_et(2026, 8, 27, 18, 0))
    assert stale.covered is False
    assert stale.pinged is False and pinger.urls == []
    fresh, pinger = _run(root, mint=_et(2026, 8, 30, 19, 30))
    assert fresh.covered is True
    assert fresh.pinged is True and pinger.urls == [URL]


def test_every_failure_is_named_at_once(fixture_lake):
    root = _clean_lake(fixture_lake)
    fixture_lake.partition_path("quotes", "SPY", date(2026, 8, 28)).unlink()
    outcome, _ = _run(root, schedule="", canary=lambda: False, mint=_et(2026, 8, 20, 12, 0))
    kinds = [p.split(":")[0].split(" ")[0] for p in outcome.problems]
    assert kinds == ["scrub", "weekday", "canary", "token"]
    assert outcome.pinged is False
