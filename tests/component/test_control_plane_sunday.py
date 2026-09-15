"""The Sunday maintenance job across one real boundary: the filesystem.

The two scrubs read a lake the fixture builder put on disk and a copy of it beside
that lake. Everything else is injected: ``now``, the calendar, the schedule reader, the
canary, the pinger, and the mint time. The rules under test: the ping fires only when
both scrubs, the canary, and the coverage assertion pass, pmset alarm drift rides the
report and never withholds the ping, an unreadable mint time is a problem and never a
skip, and every finding is named at once.

The backup copy is taken while the lake is clean, so a test that then corrupts the lake
is exercising the primary scrub alone. The copy is a plain file copy, never an
``rsync``. ``tests/conftest.py`` fails any test that shells out to one.
"""

from __future__ import annotations

import io
import shutil
import subprocess
import urllib.error
from datetime import date
from pathlib import Path

from lake import control_plane as cp
from lake.alert import Publisher
from lake.metadata import stamp_assertion_pid
from tests.support.backup import mirror_lake
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock
from tests.support.lake import FixtureLake
from tests.support.pinger import FakePinger
from tests.support.transport import FakeTransport

CALENDAR = weekday_sessions(date(2026, 8, 31), date(2026, 9, 7))
SUNDAY_20 = et(2026, 8, 30, 20, 0)
SUNDAY_19 = et(2026, 8, 30, 19, 0)
FRESH_MINT = et(2026, 8, 30, 19, 30)
URL = "https://hc-ping.com/secret-key/sunday"

REPEAT_ONLY = "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n"
BOTH = REPEAT_ONLY + "Scheduled power events:\n [0]  wakepoweron at 08/30/26 19:55:00 by 'pmset'\n"


def _backup_of(lake_root: Path) -> Path:
    """Where a lake's backup copy sits. A sibling of the lake, standing in for the SSD.

    Every call site needs one, because ``sunday_maintenance`` gives ``backup_target``
    no default. A lake that was never built has no copy, so this names a directory that
    is not there and the scrub reports it, which is the honest reading of a machine
    carrying neither.
    """
    return Path(lake_root).parent / "ssd"


def _clean_lake(fixture_lake: FixtureLake) -> Path:
    fixture_lake.with_chains("SPY", date(2026, 8, 28))
    fixture_lake.with_quotes("SPY", date(2026, 8, 28))
    root = fixture_lake.build()
    mirror_lake(root, _backup_of(root))
    return root


def _passing_canary() -> bool:
    """A canary that answers True.

    The seam carries no default, so every call site names one. A default that answered
    True without calling anything is the thing production must not have.
    """
    return True


def _run(
    lake_root: Path, *, now=SUNDAY_20, schedule=REPEAT_ONLY, canary=_passing_canary, mint=FRESH_MINT
):
    pinger = FakePinger()
    outcome = cp.sunday_maintenance(
        lake_root=lake_root,
        backup_target=_backup_of(lake_root),
        now=now,
        calendar=CALENDAR,
        schedule_reader=lambda: schedule,
        pinger=pinger,
        ping_url=URL,
        mint=mint,
        canary=canary,
    )
    return outcome, pinger


def test_clean_scrub_and_the_repeat_alarm_ping_at_the_maintenance_time(fixture_lake):
    # At Sunday 20:00 the one-shot has fired and left the schedule. Only the repeat
    # alarm is expected, per the design's read-back caveat.
    outcome, pinger = _run(_clean_lake(fixture_lake))
    assert outcome.scrub.ok and outcome.canary_passed
    assert outcome.alarms.repeat_ok and outcome.alarms.one_shot_ok
    assert outcome.covered is True
    assert outcome.pinged is True
    assert outcome.problems == () and outcome.report == ()
    assert pinger.urls == [URL]


def test_before_the_wake_a_missing_one_shot_rides_the_report(fixture_lake):
    # Both alarms are expected before the wake. A missing one is drift, which the
    # design pins to the nightly report, so it is named but the ping still fires.
    root = _clean_lake(fixture_lake)
    missing, pinger = _run(root, now=SUNDAY_19, schedule=REPEAT_ONLY)
    assert missing.alarms.one_shot_ok is False
    assert any("one-shot" in line for line in missing.report)
    assert missing.problems == ()
    assert missing.pinged is True and pinger.urls == [URL]
    present, pinger = _run(root, now=SUNDAY_19, schedule=BOTH)
    assert present.report == ()
    assert present.pinged is True and pinger.urls == [URL]


def test_a_scrub_failure_alone_owes_no_reminder(fixture_lake):
    # The reminder keys on the canary and the coverage assertion, never on whether
    # the run pinged. A corrupted partition withholds the ping and owes no reminder,
    # because re-authing would fix nothing.
    root = _clean_lake(fixture_lake)
    next(root.glob("chains/**/*.parquet")).write_bytes(b"corrupt")
    outcome, pinger = _run(root, now=SUNDAY_20, mint=FRESH_MINT)
    assert outcome.pinged is False and pinger.urls == []
    assert outcome.reminder is None


def test_a_failed_scrub_blocks_the_ping(fixture_lake):
    root = _clean_lake(fixture_lake)
    # Corrupt a manifested partition so the forward pass sees a sha mismatch.
    fixture_lake.partition_path("chains", "SPY", date(2026, 8, 28)).write_bytes(b"torn")
    outcome, pinger = _run(root)
    assert outcome.scrub.ok is False
    assert outcome.pinged is False
    assert pinger.urls == []
    assert any(p.startswith("scrub failed") for p in outcome.problems)


# -- the backup copy -----------------------------------------------------------------

# The Sunday job scrubs both copies. Until it did, ``rsync --checksum`` was the only
# thing that would have noticed the backup rotting, and it paid for that every day.


def test_a_rotted_backup_withholds_the_ping_while_the_lake_scrubs_clean(fixture_lake):
    root = _clean_lake(fixture_lake)
    partition = next((_backup_of(root)).glob("chains/**/*.parquet"))
    partition.write_bytes(b"rot")

    outcome, pinger = _run(root)
    # The lake is fine and the copy is not, which is the pair naming the side.
    assert outcome.scrub.ok is True
    assert outcome.backup.ok is False
    assert outcome.pinged is False and pinger.urls == []
    assert any(p.startswith("backup scrub failed") for p in outcome.problems)


def test_a_backup_file_gone_withholds_the_ping(fixture_lake):
    root = _clean_lake(fixture_lake)
    next((_backup_of(root)).glob("chains/**/*.parquet")).unlink()

    outcome, pinger = _run(root)
    assert outcome.backup.missing != ()
    assert outcome.pinged is False and pinger.urls == []


def test_an_unmounted_backup_target_withholds_the_ping(fixture_lake):
    # A week the scrub could not run is a week nothing looked at the copy, so this is a
    # problem rather than a skip. It is the same rule the compaction job applies when it
    # refuses to sync to an unplugged drive.
    root = _clean_lake(fixture_lake)
    shutil.rmtree(_backup_of(root))

    outcome, pinger = _run(root)
    assert outcome.backup.target_missing is True
    assert outcome.pinged is False and pinger.urls == []
    assert any("backup target not mounted" in p for p in outcome.problems)


def test_an_orphan_on_the_copy_is_named_and_still_pings(fixture_lake):
    # macOS writes to a mounted external volume without anyone asking, so an orphan that
    # withheld the ping would page on a folder someone opened in Finder. An extra file
    # on the copy costs space rather than data, so it rides the report.
    root = _clean_lake(fixture_lake)
    (_backup_of(root) / ".DS_Store").write_bytes(b"finder state")

    outcome, pinger = _run(root)
    assert outcome.backup.orphans == (".DS_Store",)
    assert outcome.problems == ()
    assert outcome.pinged is True and pinger.urls == [URL]
    assert "backup file the lake never recorded: .DS_Store" in outcome.report


def test_the_path_of_a_wrong_backup_file_reaches_the_report(fixture_lake):
    # The problem line carries a count, which decides the ping. Only a path says where
    # to look, and an operator handed "sha_mismatches=1" and nothing else cannot act.
    root = _clean_lake(fixture_lake)
    partition = next((_backup_of(root)).glob("chains/**/*.parquet"))
    partition.write_bytes(b"rot")
    rel = partition.relative_to(_backup_of(root)).as_posix()

    outcome, _ = _run(root)
    assert f"backup file does not match the lake: {rel}" in outcome.report


def test_a_backup_merely_behind_the_lake_still_pings(fixture_lake):
    # The edge that decides whether this check is worth having. The copy is written at
    # close+15 and scrubbed on Sunday, so a partition sealed in between is legitimately
    # absent. Calling that loss would withhold the ping every week.
    root = _clean_lake(fixture_lake)
    fixture_lake.with_chains("SPY", date(2026, 9, 4))
    fixture_lake.build()

    outcome, pinger = _run(root)
    assert outcome.backup.ok is True
    assert outcome.backup.pending != () and outcome.backup.missing == ()
    assert outcome.problems == ()
    assert outcome.pinged is True and pinger.urls == [URL]
    # Named all the same, at report tier, and the count is how far behind it is rather
    # than a fixed line that says nothing.
    assert "backup behind the lake by 1 partitions" in outcome.report

    fixture_lake.with_chains("SPY", date(2026, 9, 3))
    fixture_lake.build()
    behind, pinger = _run(root)
    assert "backup behind the lake by 2 partitions" in behind.report
    assert behind.pinged is True


def test_a_missing_lake_root_is_a_failure_not_a_clean_scrub(tmp_path):
    outcome, pinger = _run(tmp_path / "nowhere")
    assert outcome.pinged is False
    assert pinger.urls == []
    assert any("lake root missing" in p for p in outcome.problems)
    # A machine with no lake has no copy of one either, and both are named.
    assert any("backup target not mounted" in p for p in outcome.problems)


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
    # The reminder says so too, because an unreadable mint means the token file itself
    # is the problem rather than a skipped ritual.
    assert outcome.reminder is not None
    assert outcome.reminder.body.endswith("The token's mint time could not be read.")
    assert pinger.urls == []


def test_a_stale_mint_blocks_the_ping_and_a_fresh_one_passes(fixture_lake):
    root = _clean_lake(fixture_lake)
    stale, pinger = _run(root, mint=et(2026, 8, 27, 18, 0))
    assert stale.covered is False
    assert stale.pinged is False and pinger.urls == []
    fresh, pinger = _run(root, mint=et(2026, 8, 30, 19, 30))
    assert fresh.covered is True
    assert fresh.pinged is True and pinger.urls == [URL]


def test_every_failure_is_named_at_once(fixture_lake):
    root = _clean_lake(fixture_lake)
    fixture_lake.partition_path("quotes", "SPY", date(2026, 8, 28)).unlink()
    outcome, _ = _run(root, schedule="", canary=lambda: False, mint=et(2026, 8, 20, 12, 0))
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
            backup_target=_backup_of(root),
            now=SUNDAY_20,
            calendar=CALENDAR,
            schedule_reader=_raise(exc),
            pinger=pinger,
            ping_url=URL,
            canary=_passing_canary,
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


# -- the canary retry ----------------------------------------------------------------

# The ritual can be done any time on Sunday evening. A 20:00 run that finds no fresh
# token must not strand a re-login done at 20:10, so the attempt repeats every 30
# minutes until it passes or 23:00. Before this the plist fired once and any later
# ritual paged.

STALE_MINT = et(2026, 8, 27, 18, 0)  # last week's late mint: valid, not fresh


class _Mints:
    """Hands back one mint per attempt, repeating the last once the list runs out."""

    def __init__(self, *mints):
        self.mints = list(mints)
        self.reads = 0

    def __call__(self):
        mint = self.mints[min(self.reads, len(self.mints) - 1)]
        self.reads += 1
        return mint


def _retry_run(lake_root, *, start, mints, canary=None, schedule=REPEAT_ONLY, reminder_sink=None):
    clock = ManualClock(start=start)
    pinger = FakePinger()
    outcomes = cp.sunday_run(
        lake_root=lake_root,
        backup_target=_backup_of(lake_root),
        clock=clock,
        calendar=CALENDAR,
        schedule_reader=lambda: schedule,
        pinger=pinger,
        ping_url=URL,
        mint_reader=mints,
        canary=canary if canary is not None else _passing_canary,
        reminder_sink=reminder_sink,
    )
    return outcomes, pinger, clock


def test_the_retry_loop_scrubs_the_copy_it_was_handed(fixture_lake):
    # A clean lake scrubbed as its own backup comes back clean, so a target that never
    # reached ``sunday_maintenance`` is invisible from every test that starts from a
    # clean pair. This damages the copy alone, which only the real target can see.
    root = _clean_lake(fixture_lake)
    next((_backup_of(root)).glob("chains/**/*.parquet")).write_bytes(b"rot")

    outcomes, pinger, _ = _retry_run(root, start=SUNDAY_20, mints=_Mints(FRESH_MINT))
    assert pinger.urls == []
    assert all(o.scrub.ok for o in outcomes)
    assert all(any(p.startswith("backup scrub failed") for p in o.problems) for o in outcomes)


def test_a_healthy_sunday_makes_one_attempt(fixture_lake):
    outcomes, pinger, clock = _retry_run(
        _clean_lake(fixture_lake), start=SUNDAY_20, mints=_Mints(FRESH_MINT)
    )
    assert len(outcomes) == 1
    assert outcomes[-1].pinged is True
    assert pinger.urls == [URL]
    assert clock.now() == SUNDAY_20  # nothing slept


def test_a_ritual_done_after_the_first_run_still_clears_the_check(fixture_lake):
    # 20:00 finds last week's token. The re-login lands at 20:10. The 20:30 attempt
    # reads the mint afresh, so the ping fires and nothing pages at 23:00.
    mints = _Mints(STALE_MINT, FRESH_MINT)
    outcomes, pinger, clock = _retry_run(_clean_lake(fixture_lake), start=SUNDAY_20, mints=mints)
    assert len(outcomes) == 2
    assert outcomes[0].covered is False and outcomes[0].pinged is False
    assert outcomes[1].covered is True and outcomes[1].pinged is True
    assert pinger.urls == [URL]
    assert clock.now() == et(2026, 8, 30, 20, 30)
    assert mints.reads == 2  # the mint is read once per attempt, never cached


def test_a_ritual_never_done_retries_to_the_deadline_and_never_pings(fixture_lake):
    outcomes, pinger, clock = _retry_run(
        _clean_lake(fixture_lake), start=SUNDAY_20, mints=_Mints(STALE_MINT)
    )
    # 20:00 through 23:00 on the half hour: the deadline is the last retry.
    assert len(outcomes) == 7
    assert clock.now() == et(2026, 8, 30, 23, 0)
    assert all(o.pinged is False for o in outcomes)
    assert pinger.urls == []


def test_a_failing_canary_retries_the_same_way(fixture_lake):
    outcomes, pinger, _ = _retry_run(
        _clean_lake(fixture_lake),
        start=SUNDAY_20,
        mints=_Mints(FRESH_MINT),
        canary=lambda: False,
    )
    assert len(outcomes) == 7
    assert pinger.urls == []


def test_a_monday_catch_up_makes_one_attempt(fixture_lake):
    # launchd coalesces a wake missed over the weekend and fires the job Monday
    # morning. Retrying all day would page nobody sooner, so the catch-up runs once.
    outcomes, pinger, clock = _retry_run(
        _clean_lake(fixture_lake), start=et(2026, 8, 31, 8, 25), mints=_Mints(STALE_MINT)
    )
    assert len(outcomes) == 1
    assert pinger.urls == []
    assert clock.now() == et(2026, 8, 31, 8, 25)


def test_a_sunday_run_before_the_maintenance_time_makes_one_attempt(fixture_lake):
    # RunAtLoad is off for the Sunday plist, so an off-window start means a hand-run
    # or a coalesced catch-up. Either way only the evening window retries.
    outcomes, _, clock = _retry_run(
        _clean_lake(fixture_lake), start=et(2026, 8, 30, 15, 0), mints=_Mints(STALE_MINT)
    )
    assert len(outcomes) == 1
    assert clock.now() == et(2026, 8, 30, 15, 0)


# -- a ping that fails ---------------------------------------------------------------

# The ping is the last step. A raise there used to abort before the outcome printed, so
# a wifi blip cost the report-tier lines and the verdict as well as the ping itself.


class _RaisingPinger:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc
        self.urls: list[str] = []

    def ping(self, url: str) -> None:
        self.urls.append(url)
        raise self.exc


def test_a_failed_ping_is_a_named_problem_and_the_run_still_reports(fixture_lake):
    pinger = _RaisingPinger(urllib.error.URLError(OSError("connection refused")))
    root = _clean_lake(fixture_lake)
    outcome = cp.sunday_maintenance(
        lake_root=root,
        backup_target=_backup_of(root),
        now=SUNDAY_20,
        calendar=CALENDAR,
        schedule_reader=lambda: REPEAT_ONLY,
        pinger=pinger,
        ping_url=URL,
        canary=_passing_canary,
        mint=FRESH_MINT,
    )
    # It was attempted, it failed, and the failure is named beside the other findings.
    assert pinger.urls == [URL]
    assert outcome.pinged is False
    assert outcome.problems == ("ping failed: URLError",)
    # Everything the run learned survives, which a raise would have taken with it.
    assert outcome.scrub.ok and outcome.canary_passed and outcome.covered is True


def test_a_failed_ping_never_carries_the_key(fixture_lake):
    pinger = _RaisingPinger(urllib.error.HTTPError(URL, 500, "Server Error", {}, io.BytesIO(b"")))
    root = _clean_lake(fixture_lake)
    outcome = cp.sunday_maintenance(
        lake_root=root,
        backup_target=_backup_of(root),
        now=SUNDAY_20,
        calendar=CALENDAR,
        schedule_reader=lambda: REPEAT_ONLY,
        pinger=pinger,
        ping_url=URL,
        canary=_passing_canary,
        mint=FRESH_MINT,
    )
    # The assertion uses equality rather than scanning for the key. Equality is stronger
    # claim: it says what the line is, so nothing else can be in it.
    assert outcome.problems == ("ping failed: HTTPError",)


def test_the_retry_loop_gives_a_failed_ping_another_chance(fixture_lake):
    # The Sunday job already retries every 30 minutes, so a blip at 20:00 is retried at
    # 20:30 with no HTTP-level retry in the pinger.
    clock = ManualClock(start=SUNDAY_20)
    pinger = _RaisingPinger(TimeoutError("timed out"))
    root = _clean_lake(fixture_lake)
    outcomes = cp.sunday_run(
        lake_root=root,
        backup_target=_backup_of(root),
        clock=clock,
        calendar=CALENDAR,
        schedule_reader=lambda: REPEAT_ONLY,
        pinger=pinger,
        ping_url=URL,
        mint_reader=_Mints(FRESH_MINT),
        canary=_passing_canary,
    )
    assert len(outcomes) == 7
    assert len(pinger.urls) == 7
    assert all(o.problems == ("ping failed: TimeoutError",) for o in outcomes)


# -- the Time Machine exclusion ------------------------------------------------------

# A sticky exclusion is invisible once set and dies quietly when the item it marks is
# replaced. config.yaml is hand-edited and most editors save by rename, so something
# has to look. A lost exclusion rides the report, like pmset drift.

CONFIG_DIR = "/Users/someone/.config/marketlake"


def _excluded(*paths: str) -> str:
    return "".join(f"[Excluded]\t{p}\n" for p in paths)


def _exclusion_run(lake_root, *, reader, targets=(CONFIG_DIR,)):
    pinger = FakePinger()
    outcome = cp.sunday_maintenance(
        lake_root=lake_root,
        backup_target=_backup_of(lake_root),
        now=SUNDAY_20,
        calendar=CALENDAR,
        schedule_reader=lambda: REPEAT_ONLY,
        pinger=pinger,
        ping_url=URL,
        canary=_passing_canary,
        mint=FRESH_MINT,
        exclusion_targets=targets,
        exclusion_reader=reader,
    )
    return outcome, pinger


def test_an_excluded_config_directory_reports_nothing(fixture_lake):
    outcome, pinger = _exclusion_run(
        _clean_lake(fixture_lake), reader=lambda paths: _excluded(*paths)
    )
    assert outcome.report == ()
    assert outcome.pinged is True and pinger.urls == [URL]


def test_a_lost_exclusion_rides_the_report_and_the_ping_still_fires(fixture_lake):
    outcome, pinger = _exclusion_run(
        _clean_lake(fixture_lake),
        reader=lambda paths: "".join(f"[Included]\t{p}\n" for p in paths),
    )
    assert any("not excluded from time machine" in line for line in outcome.report)
    assert CONFIG_DIR in outcome.report[0]
    assert outcome.problems == ()
    assert outcome.pinged is True and pinger.urls == [URL]


def test_a_raising_or_unreadable_exclusion_probe_rides_the_report(fixture_lake):
    for reader in (
        lambda paths: (_ for _ in ()).throw(FileNotFoundError(2, "no tmutil", "tmutil")),
        lambda paths: "a shape nobody has seen\n",
    ):
        outcome, pinger = _exclusion_run(_clean_lake(fixture_lake), reader=reader)
        assert any("exclusion unreadable" in line for line in outcome.report)
        assert outcome.problems == ()
        assert outcome.pinged is True and pinger.urls == [URL]


def test_a_path_tmutil_cannot_resolve_is_not_an_exclusion(fixture_lake):
    outcome, _ = _exclusion_run(
        _clean_lake(fixture_lake), reader=lambda paths: f"[UNKNOWN]\t{paths[0]}\n"
    )
    assert any("not excluded from time machine" in line for line in outcome.report)


def test_every_target_is_checked(fixture_lake):
    # A token outside the config directory carries its own exclusion, so both are read.
    token = "/elsewhere/token.json"
    outcome, _ = _exclusion_run(
        _clean_lake(fixture_lake),
        targets=(CONFIG_DIR, token),
        reader=lambda paths: _excluded(CONFIG_DIR) + f"[Included]\t{token}\n",
    )
    assert len(outcome.report) == 1
    assert token in outcome.report[0]


# -- the Sunday re-auth reminder -----------------------------------------------------

# The build plan assigns the reminder to this deliverable. It fires on Sunday only, on
# the 20:00, 21:00, and 22:00 runs, while the throwaway call or the coverage assertion
# still fails. So it never fires midweek, never on the half-hour retries, and stops on
# its own once the ritual is done. Delivery is the publisher's, through
# `reminder_publisher`. The decision is here, and the sink is a recorder.


def _reminders(lake_root, *, start, mints, canary=None):
    """Every reminder `sunday_run` sends across one evening."""
    sent = []
    _retry_run(lake_root, start=start, mints=mints, canary=canary, reminder_sink=sent.append)
    return sent


def test_three_reminders_go_out_across_a_failing_sunday_evening(fixture_lake):
    sent = _reminders(_clean_lake(fixture_lake), start=SUNDAY_20, mints=_Mints(STALE_MINT))
    # Seven attempts run, on the hour and the half hour. Only the three on the hour
    # send, and 23:00 is a retry rather than a reminder.
    assert len(sent) == 3
    assert all(r.title == "Sunday re-auth due" and r.priority == 3 for r in sent)
    assert all("Token minted 2026-08-27." in r.body for r in sent)
    assert all("The coverage assertion failed." in r.body for r in sent)


def test_the_reminder_stops_once_the_ritual_is_done(fixture_lake):
    # The re-login lands at 20:10. The 20:00 reminder went out. The 20:30 attempt
    # passes, so nothing further is owed.
    sent = _reminders(
        _clean_lake(fixture_lake), start=SUNDAY_20, mints=_Mints(STALE_MINT, FRESH_MINT)
    )
    assert len(sent) == 1


def test_a_healthy_sunday_sends_no_reminder(fixture_lake):
    assert _reminders(_clean_lake(fixture_lake), start=SUNDAY_20, mints=_Mints(FRESH_MINT)) == []


def test_a_midweek_run_sends_no_reminder(fixture_lake):
    # RunAtLoad is off for the Sunday plist, so this needs a hand-run to happen at
    # all. It must still stay quiet.
    sent = _reminders(
        _clean_lake(fixture_lake), start=et(2026, 8, 31, 21, 0), mints=_Mints(STALE_MINT)
    )
    assert sent == []


def test_the_outcome_carries_the_reminder_even_with_no_sink(fixture_lake):
    # The decision must not depend on a sink being wired. A caller with no sink still
    # gets the reminder on the outcome, and the command line still logs the body.
    outcome, _ = _run(_clean_lake(fixture_lake), mint=STALE_MINT)
    assert outcome.reminder is not None
    assert outcome.reminder.body.startswith("The coverage assertion failed.")


def test_a_failing_canary_names_both_halves(fixture_lake):
    outcome, _ = _run(_clean_lake(fixture_lake), mint=STALE_MINT, canary=lambda: False)
    assert outcome.reminder is not None
    assert outcome.reminder.body == (
        "The throwaway call and the coverage assertion failed. Token minted 2026-08-27."
    )


# -- the daemon-liveness page ---------------------------------------------------------

# Deadman coverage stops on the weekend on purpose, per its own docstring, and the
# daemon's own re-take heals a lost assertion within a minute of it going, per
# ``daemon.py``'s ``report_lost_assertion``. What is left unwatched is the daemon being
# gone entirely through the Sunday window, which nothing else can notice or page for.
# These tests drive ``sunday_maintenance`` and ``sunday_run`` with the same two seams
# ``self_check`` takes: a daemon probe and an assertion probe matched against a stamped
# pid.

_DAEMON_PID = 4242
SATURDAY = et(2026, 9, 5, 12, 0)  # outside every assertion window: nothing is owed


def _daemon_run(
    lake_root,
    *,
    now=SUNDAY_20,
    daemon_probe,
    assertion_probe=None,
    assertion_pid=None,
):
    pinger = FakePinger()
    outcome = cp.sunday_maintenance(
        lake_root=lake_root,
        backup_target=_backup_of(lake_root),
        now=now,
        calendar=CALENDAR,
        schedule_reader=lambda: REPEAT_ONLY,
        pinger=pinger,
        ping_url=URL,
        canary=_passing_canary,
        mint=FRESH_MINT,
        daemon_probe=daemon_probe,
        assertion_probe=assertion_probe,
        assertion_pid=assertion_pid,
    )
    return outcome, pinger


def test_a_dead_daemon_pages_and_names_the_daemon_rather_than_the_assertion(fixture_lake):
    root = _clean_lake(fixture_lake)
    asked: list[int] = []
    outcome, pinger = _daemon_run(
        root,
        daemon_probe=lambda label: False,
        assertion_probe=lambda pid: asked.append(pid) or True,
    )
    assert outcome.daemon_page is not None
    assert outcome.daemon_page.event == cp.SUNDAY_DAEMON_DOWN_EVENT
    assert cp.DAEMON_LABEL in outcome.daemon_page.body
    # A daemon already known dead is not also asked about its assertion. Naming the
    # assertion here would send an operator to the wrong repair.
    assert asked == []
    # The finding rides the report as well as the page.
    assert outcome.daemon_page.title in outcome.report
    # Report-tier, not problems: it does not withhold the ping on its own.
    assert outcome.problems == ()
    assert outcome.pinged is True and pinger.urls == [URL]


def test_a_live_daemon_holding_nothing_pages(fixture_lake):
    root = _clean_lake(fixture_lake)
    stamp_assertion_pid(root, pid=_DAEMON_PID)
    outcome, pinger = _daemon_run(
        root,
        daemon_probe=lambda label: True,
        assertion_probe=lambda pid: False,
        assertion_pid=_DAEMON_PID,
    )
    assert outcome.daemon_page is not None
    assert outcome.daemon_page.event == cp.SUNDAY_ASSERTION_UNHELD_EVENT
    assert str(_DAEMON_PID) in outcome.daemon_page.body
    assert outcome.daemon_page.title in outcome.report
    assert outcome.pinged is True and pinger.urls == [URL]


def test_a_live_daemon_holding_the_assertion_under_a_different_pid_pages(fixture_lake):
    # The probe matches identity, not mere presence, the same rule #218 gave the
    # weekday half: a caffeinate belonging to someone else does not satisfy this.
    root = _clean_lake(fixture_lake)
    stamp_assertion_pid(root, pid=_DAEMON_PID)

    def probe(pid: int) -> bool:
        assert pid == _DAEMON_PID
        return False  # pmset holds an assertion, but under a different pid

    outcome, _ = _daemon_run(
        root, daemon_probe=lambda label: True, assertion_probe=probe, assertion_pid=_DAEMON_PID
    )
    assert outcome.daemon_page is not None
    assert outcome.daemon_page.event == cp.SUNDAY_ASSERTION_UNHELD_EVENT


def test_a_missing_stamp_reads_as_the_assertion_finding(fixture_lake):
    # No pid was ever stamped. `self_check` treats this as the second state rather
    # than a skip, and this mirrors it.
    root = _clean_lake(fixture_lake)
    outcome, _ = _daemon_run(
        root, daemon_probe=lambda label: True, assertion_probe=lambda pid: True, assertion_pid=None
    )
    assert outcome.daemon_page is not None
    assert outcome.daemon_page.event == cp.SUNDAY_ASSERTION_UNHELD_EVENT
    assert "no pid recorded" in outcome.daemon_page.body


def test_a_healthy_run_pages_nothing(fixture_lake):
    root = _clean_lake(fixture_lake)
    stamp_assertion_pid(root, pid=_DAEMON_PID)
    outcome, pinger = _daemon_run(
        root,
        daemon_probe=lambda label: True,
        assertion_probe=lambda pid: True,
        assertion_pid=_DAEMON_PID,
    )
    assert outcome.daemon_page is None
    assert outcome.report == ()
    assert outcome.pinged is True and pinger.urls == [URL]


def test_outside_the_assertion_window_neither_probe_is_asked(fixture_lake):
    # A Saturday hand-run: nothing is owed, so the scrub proceeds and asks for no
    # assertion, the same rule ``self_check`` already follows for its own probe.
    root = _clean_lake(fixture_lake)
    calls: list[str] = []
    outcome, _ = _daemon_run(
        root,
        now=SATURDAY,
        daemon_probe=lambda label: calls.append("daemon") or False,
        assertion_probe=lambda pid: calls.append("assertion") or False,
        assertion_pid=_DAEMON_PID,
    )
    assert calls == []
    assert outcome.daemon_page is None


def _publisher(lake_root, transport, *, secrets=()):
    return Publisher(lake_root=lake_root, transport=transport, secrets=secrets)


def test_the_same_failure_on_a_retry_does_not_page_twice(fixture_lake):
    # Seven attempts run from 20:00 to 23:00 while the daemon stays dead the whole
    # evening. One page for the window, not one per attempt.
    root = _clean_lake(fixture_lake)
    transport = FakeTransport()
    clock = ManualClock(start=SUNDAY_20)
    outcomes = cp.sunday_run(
        lake_root=root,
        backup_target=_backup_of(root),
        clock=clock,
        calendar=CALENDAR,
        schedule_reader=lambda: REPEAT_ONLY,
        pinger=FakePinger(),
        ping_url=URL,
        mint_reader=_Mints(STALE_MINT),
        canary=_passing_canary,
        daemon_probe=lambda label: False,
        publisher=_publisher(root, transport),
    )
    assert len(outcomes) == 7
    assert all(o.daemon_page is not None for o in outcomes)
    assert len(transport.messages) == 1
    assert transport.messages[0].event == cp.SUNDAY_DAEMON_DOWN_EVENT


def test_the_assertion_pid_is_read_fresh_each_retry(fixture_lake):
    # The daemon restamps a new pid when it re-takes a lost caffeinate mid-evening. A
    # pid cached once at the start of the run would then ask about a child already gone
    # and page a lapse that had already healed, the exact cry-wolf shape the daemon's
    # own re-take exists to avoid. Reading the pid fresh each attempt, the way the mint
    # already is, is what prevents it.
    root = _clean_lake(fixture_lake)
    # ``mints.reads`` is incremented by ``mint_reader()``, which ``sunday_run`` always
    # calls before ``assertion_pid_reader()`` within the same attempt, so it is a safe
    # stand-in for "which attempt this is" from both readers below.
    mints = _Mints(STALE_MINT)

    def read_pid() -> int:
        # The restamp lands between the first attempt and the second.
        return 4242 if mints.reads <= 1 else 9999

    def held(pid: int) -> bool:
        # pmset holds an assertion only under the pid currently real. The first
        # child's process is gone once the restamp has happened.
        current = 4242 if mints.reads <= 1 else 9999
        return pid == current

    clock = ManualClock(start=SUNDAY_20)
    outcomes = cp.sunday_run(
        lake_root=root,
        backup_target=_backup_of(root),
        clock=clock,
        calendar=CALENDAR,
        schedule_reader=lambda: REPEAT_ONLY,
        pinger=FakePinger(),
        ping_url=URL,
        mint_reader=mints,
        canary=_passing_canary,
        daemon_probe=lambda label: True,
        assertion_probe=held,
        assertion_pid_reader=read_pid,
    )
    assert mints.reads == 7
    assert len(outcomes) == 7
    assert all(o.daemon_page is None for o in outcomes), (
        "a stale pid paged a lapse that had already healed"
    )


def test_a_refused_page_keeps_its_body_off_stderr(fixture_lake, capsys):
    # The bargain ``compact._page_drift`` already makes: a publisher that refused the
    # page found one of its own secrets in the body, and stderr must not undo that
    # redaction by printing the body anyway.
    root = _clean_lake(fixture_lake)
    transport = FakeTransport()
    clock = ManualClock(start=SUNDAY_20)
    cp.sunday_run(
        lake_root=root,
        backup_target=_backup_of(root),
        clock=clock,
        calendar=CALENDAR,
        schedule_reader=lambda: REPEAT_ONLY,
        pinger=FakePinger(),
        ping_url=URL,
        mint_reader=_Mints(FRESH_MINT),
        canary=_passing_canary,
        daemon_probe=lambda label: False,
        # A publisher configured to treat the daemon label as a secret, so the page
        # this run raises is refused the way a page carrying a real one would be.
        publisher=_publisher(root, transport, secrets=(cp.DAEMON_LABEL,)),
    )
    assert transport.messages == []
    err = capsys.readouterr().err
    assert "refused: it carried a secret" in err
    assert cp.DAEMON_LABEL not in err
