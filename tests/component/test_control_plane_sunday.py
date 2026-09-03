"""The Sunday maintenance job across one real boundary: the filesystem.

The scrub reads a lake the fixture builder put on disk. Everything else is injected:
``now``, the calendar, the schedule reader, the canary, the pinger, and the mint time.
The rules under test: the ping fires only when the scrub, the canary, and the
coverage assertion pass, pmset alarm drift rides the report and never withholds the
ping, an unreadable mint time is a problem and never a skip, and every finding is
named at once.
"""

from __future__ import annotations

import subprocess
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
FRESH_MINT = _et(2026, 8, 30, 19, 30)
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


def _run(lake_root: Path, *, now=SUNDAY_20, schedule=REPEAT_ONLY, canary=None, mint=FRESH_MINT):
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
    assert outcome.covered is True
    assert outcome.pinged is True
    assert outcome.problems == () and outcome.report == ()
    assert pinger.urls == [URL]


def test_before_the_wake_a_missing_one_shot_rides_the_report(fixture_lake):
    # Both alarms are expected before the wake. A missing one is drift, which the
    # design pins to the nightly report, so it is named but the ping still fires.
    root = _clean_lake(fixture_lake)
    missing, pinger = _run(root, now=SUNDAY_19, schedule=REPEAT_ONLY)
    assert missing.alarms.ok is False
    assert any("one-shot" in line for line in missing.report)
    assert missing.problems == ()
    assert missing.pinged is True and pinger.urls == [URL]
    present, pinger = _run(root, now=SUNDAY_19, schedule=BOTH)
    assert present.report == ()
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


def test_a_missing_repeat_alarm_rides_the_report_and_the_ping_still_fires(fixture_lake):
    # The pre-open self-check catches a missed wake an hour before the bell, so
    # waiting overnight loses nothing that page does not already cover.
    outcome, pinger = _run(_clean_lake(fixture_lake), schedule="No scheduled events.\n")
    assert outcome.alarms.repeat_ok is False
    assert any("repeat" in line for line in outcome.report)
    assert outcome.problems == ()
    assert outcome.pinged is True
    assert pinger.urls == [URL]


def test_a_failing_canary_blocks_the_ping(fixture_lake):
    outcome, pinger = _run(_clean_lake(fixture_lake), canary=lambda: False)
    assert outcome.canary_passed is False
    assert outcome.pinged is False
    assert pinger.urls == []
    assert "canary call failed" in outcome.problems


def test_an_unreadable_mint_is_a_problem_not_a_skip(fixture_lake):
    outcome, pinger = _run(_clean_lake(fixture_lake), mint=None)
    assert outcome.covered is None
    assert any("mint time unreadable" in p for p in outcome.problems)
    assert outcome.pinged is False
    assert pinger.urls == []


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
    assert kinds == ["scrub", "canary", "token"]
    assert [line.split(" ")[0] for line in outcome.report] == ["weekday"]
    assert outcome.pinged is False


# -- the read-back that cannot be read -----------------------------------------------

# The design routes the whole read-back step to the nightly report. So a reader that
# raises, and a line the parser does not know, must both ride the report and leave the
# ping alone. Before this, each aborted the run after the scrub and before the canary,
# the coverage assertion, and the ping, which paged at 23:00.


def _raise(exc: Exception):
    def reader() -> str:
        raise exc

    return reader


def test_a_reader_that_raises_rides_the_report_and_the_ping_still_fires(fixture_lake):
    root = _clean_lake(fixture_lake)
    for exc in (
        subprocess.CalledProcessError(1, ["pmset", "-g", "sched"]),
        FileNotFoundError(2, "No such file or directory", "pmset"),
        RuntimeError("the seam broke in a way nobody predicted"),
    ):
        pinger = FakePinger()
        outcome = cp.sunday_maintenance(
            lake_root=root,
            now=SUNDAY_20,
            calendar=CALENDAR,
            schedule_reader=_raise(exc),
            pinger=pinger,
            ping_url=URL,
            mint=FRESH_MINT,
        )
        assert outcome.alarms.repeat_ok is False
        assert any("read-back unreadable" in line for line in outcome.report)
        assert type(exc).__name__ in outcome.report[0]
        assert outcome.problems == ()
        assert outcome.pinged is True
        assert pinger.urls == [URL]


def test_an_unparseable_line_rides_the_report_and_the_ping_still_fires(fixture_lake):
    outcome, pinger = _run(
        _clean_lake(fixture_lake),
        schedule="Repeating power events:\n  a shape nobody has seen\n",
    )
    assert any("read-back unreadable" in line for line in outcome.report)
    assert "PmsetParseError" in outcome.report[0]
    assert outcome.problems == ()
    assert outcome.pinged is True
    assert pinger.urls == [URL]


def test_an_unnamed_day_mask_is_drift_not_an_unreadable_line(fixture_lake):
    # pmset prints "Some days" for any mask it has no name for. That is a drifted
    # alarm, so it parses and the check names it, rather than aborting the run.
    outcome, pinger = _run(
        _clean_lake(fixture_lake),
        schedule="Repeating power events:\n  wakepoweron at 8:25AM Some days\n",
    )
    assert outcome.alarms.repeat_ok is False
    assert any("drifted" in line for line in outcome.report)
    assert not any("unreadable" in line for line in outcome.report)
    assert outcome.problems == ()
    assert outcome.pinged is True
    assert pinger.urls == [URL]


def test_a_foreign_one_shot_with_a_leeway_tail_does_not_break_the_read_back(fixture_lake):
    # pmset lists every owner's events. One carrying a leeway or user-visible tail
    # must not cost this job its ping.
    schedule = (
        REPEAT_ONLY + "Scheduled power events:\n"
        " [0]  wake at 09/06/2026 03:11:52 by 'com.apple.alarm.user-invisible' leeway secs: 300\n"
        " [1]  wake at 09/06/2026 09:00:00 by 'com.apple.someagent' User visible: true\n"
    )
    outcome, pinger = _run(_clean_lake(fixture_lake), schedule=schedule)
    assert outcome.report == ()
    assert outcome.pinged is True
    assert pinger.urls == [URL]
