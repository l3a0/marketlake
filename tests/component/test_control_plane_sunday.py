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

import errno
import io
import os
import shutil
import stat
import subprocess
import urllib.error
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

import pytest

from lake import control_plane as cp
from lake import token_store
from lake.alert import Publisher
from lake.metadata import JournalMetadata, stamp_assertion_pid
from tests.support.backup import FAIL, WRONG, FakeBackupReader, mirror_lake
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock
from tests.support.fifo import without_blocking
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
    lake_root: Path,
    *,
    now=SUNDAY_20,
    schedule=REPEAT_ONLY,
    canary=_passing_canary,
    mint=FRESH_MINT,
    backup_reader=None,
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
        backup_reader=backup_reader,
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


def test_with_no_readers_the_read_backs_are_skipped_without_a_line(fixture_lake):
    """The Linux host passes neither reader, because it has no wake and no Time Machine.

    Before the wake both alarms are expected, so a ``None`` reader treated as a read that
    failed or found nothing would add a line here. A skip adds none and leaves ``alarms``
    ``None``, which says the question was not asked.
    """
    root = _clean_lake(fixture_lake)
    pinger = FakePinger()
    outcome = cp.sunday_maintenance(
        lake_root=root,
        backup_target=_backup_of(root),
        now=SUNDAY_19,
        calendar=CALENDAR,
        schedule_reader=None,
        pinger=pinger,
        ping_url=URL,
        mint=FRESH_MINT,
        canary=_passing_canary,
        exclusion_targets=("/config", "/config/token.json"),
        exclusion_reader=None,
    )
    assert outcome.alarms is None
    assert not any("pmset" in line or "read-back" in line for line in outcome.report)
    assert outcome.report == ()
    assert outcome.problems == ()
    assert outcome.pinged is True and pinger.urls == [URL]


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


# -- the restore test ----------------------------------------------------------------

# The clean lake's two partitions sit in residues 7 (chains) and 2 (quotes), computed by
# hand. SUNDAY_20 is week 34, which holds neither, so the restore wraps round to residue 2
# and reads the quotes partition alone.
CHAINS = "chains/ticker=SPY/date=2026-08-28.parquet"
QUOTES = "quotes/ticker=SPY/date=2026-08-28.parquet"


def test_a_clean_sunday_restores_the_week_s_file_from_the_target_and_pings(fixture_lake):
    root = _clean_lake(fixture_lake)
    outcome, pinger = _run(root)
    assert outcome.restore is not None
    assert outcome.restore.week == 34
    assert outcome.restore.restored == (QUOTES,)
    assert outcome.restore.ok is True
    assert outcome.restore.bytes_read == (root / QUOTES).stat().st_size
    assert outcome.restore.pass_line == (
        f"1 file (0.0 MB) read back from {_backup_of(root)} matched the manifest, "
        "week of Sunday 2026-08-30, rotation slot 34 of 52, from slot 2 because slot 34 "
        "held no files"
    )
    # A pass rides its own field, so a healthy run's report keeps its shape.
    assert outcome.problems == () and outcome.report == ()
    assert outcome.pinged is True and pinger.urls == [URL]


def test_the_default_reader_reads_the_backup_target_and_not_the_lake(fixture_lake):
    # The lake's own copy of the file week 34 reads is rotted, and the backup's is sound.
    # The primary scrub names the lake, and a restore that read the lake root instead of
    # the target would name the file a second time as a backup fault.
    root = _clean_lake(fixture_lake)
    (root / QUOTES).write_bytes(b"rot")

    outcome, _ = _run(root)

    assert outcome.scrub.sha_mismatches == (QUOTES,)
    assert outcome.backup.ok is True
    assert outcome.restore is not None
    assert outcome.restore.restored == (QUOTES,) and outcome.restore.ok is True


def test_a_monday_catch_up_restores_the_sunday_s_files(fixture_lake):
    outcome, _ = _run(_clean_lake(fixture_lake), now=et(2026, 8, 31, 8, 25))
    assert outcome.restore is not None
    assert outcome.restore.week == 34


def test_a_file_that_restores_wrong_withholds_the_ping_and_is_named(fixture_lake):
    # The copy on disk is sound, so the backup scrub passes. The bad bytes come through
    # the reader, which is the only way a path target can show the restore its own case.
    root = _clean_lake(fixture_lake)
    reader = FakeBackupReader(_backup_of(root), every=WRONG)

    outcome, pinger = _run(root, backup_reader=reader)

    assert outcome.backup.ok is True
    assert outcome.restore is not None
    assert outcome.restore.mismatches == (QUOTES,)
    assert outcome.pinged is False and pinger.urls == []
    assert f"restore test failed: mismatches=1 unreadable=0: {_backup_of(root)}" in outcome.problems
    assert f"restore read back bytes that do not match the manifest: {QUOTES}" in outcome.report
    assert any("two reads of one file returned different bytes" in line for line in outcome.report)


def test_a_failed_restore_read_withholds_the_ping_without_raising(fixture_lake):
    root = _clean_lake(fixture_lake)
    reader = FakeBackupReader(_backup_of(root), every=FAIL)

    outcome, pinger = _run(root, backup_reader=reader)

    assert outcome.pinged is False and pinger.urls == []
    # Every check after the restore still ran.
    assert outcome.canary_passed is True and outcome.covered is True
    assert f"restore test failed: mismatches=0 unreadable=1: {_backup_of(root)}" in outcome.problems
    assert f"restore could not read: {QUOTES}: OSError: fake read failed: {QUOTES}" in (
        outcome.report
    )


def test_an_unmounted_target_runs_no_restore_and_adds_no_second_line(fixture_lake):
    root = _clean_lake(fixture_lake)
    reader = FakeBackupReader(_backup_of(root), every=WRONG)
    shutil.rmtree(_backup_of(root))

    outcome, pinger = _run(root, backup_reader=reader)

    assert outcome.backup.target_missing is True
    assert outcome.restore is None
    assert reader.calls == []
    assert not any(line.startswith("restore") for line in outcome.problems + outcome.report)
    assert outcome.pinged is False


def test_a_copy_the_scrub_already_flagged_is_not_restored_again(fixture_lake):
    # The quotes copy is the file week 34 would read. Once the scrub names it, the restore
    # takes what is left, which is the chains file, and names nothing twice.
    root = _clean_lake(fixture_lake)
    (_backup_of(root) / QUOTES).write_bytes(b"rot")
    reader = FakeBackupReader(_backup_of(root))

    outcome, pinger = _run(root, backup_reader=reader)

    assert outcome.backup.sha_mismatches == (QUOTES,)
    assert reader.calls == [CHAINS]
    assert outcome.restore is not None and outcome.restore.ok is True
    assert not any(line.startswith("restore") for line in outcome.problems + outcome.report)
    assert outcome.pinged is False


def test_a_copy_wholly_flagged_by_the_scrub_leaves_nothing_to_restore(fixture_lake):
    root = _clean_lake(fixture_lake)
    (_backup_of(root) / QUOTES).write_bytes(b"rot")
    (_backup_of(root) / CHAINS).write_bytes(b"rot")
    reader = FakeBackupReader(_backup_of(root), every=WRONG)

    outcome, _ = _run(root, backup_reader=reader)

    assert reader.calls == []
    assert outcome.restore is not None and outcome.restore.ok is True
    assert outcome.restore.pass_line == (
        "nothing to restore, the backup scrub matched no files, "
        "week of Sunday 2026-08-30, rotation slot 34 of 52"
    )
    assert not any(line.startswith("restore") for line in outcome.problems)


def test_a_missing_lake_root_is_a_failure_not_a_clean_scrub(tmp_path):
    outcome, pinger = _run(tmp_path / "nowhere")
    assert outcome.pinged is False
    assert pinger.urls == []
    # One lake line. The scrub is skipped, since its walk would name the missing root again.
    assert [p for p in outcome.problems if p.startswith(("lake", "scrub"))] == [
        f"lake root missing: {tmp_path / 'nowhere'}"
    ]
    assert outcome.scrub is None
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
    assert [line.split(" ")[0] for line in outcome.report] == ["weekday", "lake"]
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


def _retry_run(
    lake_root,
    *,
    start,
    mints,
    canary=None,
    schedule=REPEAT_ONLY,
    reminder_sink=None,
    backup_reader=None,
    token_pull=None,
):
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
        token_pull=token_pull,
        reminder_sink=reminder_sink,
        backup_reader=backup_reader,
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


def test_every_attempt_restores_afresh_and_a_failing_restore_retries(fixture_lake):
    # A failure is never carried from one attempt to the next, so a reader that fails
    # all evening is asked once per attempt and the evening never pings.
    root = _clean_lake(fixture_lake)
    reader = FakeBackupReader(_backup_of(root), every=FAIL)
    outcomes, pinger, _ = _retry_run(
        root, start=SUNDAY_20, mints=_Mints(FRESH_MINT), backup_reader=reader
    )
    assert len(outcomes) == 7
    assert reader.calls == [QUOTES] * 7
    assert all(o.restore is not None and o.restore.ok is False for o in outcomes)
    assert pinger.urls == []


def test_a_path_target_reads_again_on_every_attempt_even_after_a_pass(fixture_lake):
    # A bucket target keeps a pass between attempts, because its read is a download. A
    # path target's read costs a 52nd of what the scrub re-hashes, so it keeps nothing:
    # the 20:30 retry after a stale token reads the file a second time.
    root = _clean_lake(fixture_lake)
    reader = FakeBackupReader(_backup_of(root))
    outcomes, pinger, _ = _retry_run(
        root, start=SUNDAY_20, mints=_Mints(STALE_MINT, FRESH_MINT), backup_reader=reader
    )
    assert len(outcomes) == 2
    assert reader.calls == [QUOTES, QUOTES]
    assert all(o.restore.ok and o.restore.reused is False for o in outcomes)
    assert pinger.urls == [URL]


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
        token_pull=None,
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


# -- the token pull -------------------------------------------------------------------

# On a ``store`` host each attempt copies the token parameter into ``token.json`` before
# it reads the mint, so a re-auth put on the laptop mid-evening reaches the next
# attempt's coverage assertion and canary (marketlake #702). A reminder that goes out
# names a failed pull by its outcome, and an ``unreadable`` one by its reason too, but
# never by the pull's line, which carries the token's path.

PULL_PATH = "/home/someone/.config/marketlake/token.json"


def _pulled(outcome, reason=None):
    """A pull result whose line names the token's path, as every real line does."""
    return token_store.PullResult(outcome, f"{outcome}: {PULL_PATH}", reason=reason)


class _Pulls:
    """Hands back one pull result per attempt, repeating the last once the list runs out."""

    def __init__(self, *results, events=None):
        self.results = list(results)
        self.calls = 0
        self.events = events

    def __call__(self):
        if self.events is not None:
            self.events.append("pull")
        result = self.results[min(self.calls, len(self.results) - 1)]
        self.calls += 1
        return result


def test_each_attempt_pulls_before_it_reads_the_mint_and_runs_the_canary(fixture_lake):
    events = []
    mints = _Mints(STALE_MINT, FRESH_MINT)

    def mint():
        events.append("mint")
        return mints()

    def canary():
        events.append("canary")
        return True

    outcomes, _, _ = _retry_run(
        _clean_lake(fixture_lake),
        start=SUNDAY_20,
        mints=mint,
        canary=canary,
        token_pull=_Pulls(_pulled(token_store.CURRENT), events=events),
    )
    assert len(outcomes) == 2
    assert events == ["pull", "mint", "canary"] * 2


def test_the_mint_and_the_canary_read_the_file_the_pull_wrote(fixture_lake):
    # The laptop's re-auth reached the parameter at 19:30. The local file still holds last
    # week's token until the pull copies the new one over it, so an attempt that read the
    # mint or ran the canary before pulling would fail at 20:00 and retry at 20:30.
    held = {"mint": STALE_MINT}

    def pull():
        held["mint"] = FRESH_MINT
        return _pulled(token_store.WROTE)

    canaries = []

    def canary():
        canaries.append(held["mint"])
        return held["mint"] == FRESH_MINT

    sent = []
    outcomes, pinger, _ = _retry_run(
        _clean_lake(fixture_lake),
        start=SUNDAY_20,
        mints=lambda: held["mint"],
        canary=canary,
        reminder_sink=sent.append,
        token_pull=pull,
    )
    assert len(outcomes) == 1
    assert outcomes[0].covered is True and outcomes[0].canary_passed is True
    assert outcomes[0].pull.outcome == token_store.WROTE
    assert canaries == [FRESH_MINT]
    assert pinger.urls == [URL]
    assert sent == []


def test_with_no_pull_nothing_is_pulled_and_the_reminder_is_unchanged(fixture_lake):
    sent = []
    outcomes, _, _ = _retry_run(
        _clean_lake(fixture_lake),
        start=SUNDAY_20,
        mints=_Mints(STALE_MINT),
        reminder_sink=sent.append,
        token_pull=None,
    )
    assert all(o.pull is None for o in outcomes)
    assert [r.body for r in sent] == ["The coverage assertion failed. Token minted 2026-08-27."] * 3


@pytest.mark.parametrize(
    ("pulled", "named"),
    [
        (_pulled(token_store.UNREADABLE, "ParameterNotFound"), "unreadable (ParameterNotFound)"),
        (
            _pulled(token_store.UNREADABLE, "AssumeRole AccessDenied"),
            "unreadable (AssumeRole AccessDenied)",
        ),
        (_pulled(token_store.STORE_OLDER), "store older"),
        (_pulled(token_store.NO_CREDENTIALS), "no credentials"),
        (_pulled(cp.PULL_CONFIG_REFUSED), "config refused"),
        (_pulled(cp.PULL_NOT_WRITTEN), "not written"),
    ],
    ids=lambda value: value if isinstance(value, str) else value.outcome,
)
def test_a_reminder_names_a_failed_pull_and_never_its_line(fixture_lake, pulled, named):
    sent = []
    outcomes, _, _ = _retry_run(
        _clean_lake(fixture_lake),
        start=SUNDAY_20,
        mints=_Mints(STALE_MINT),
        reminder_sink=sent.append,
        token_pull=_Pulls(pulled),
    )
    assert len(sent) == 3
    assert all(
        r.body == f"The coverage assertion failed. Token minted 2026-08-27. Token pull: {named}."
        for r in sent
    )
    assert all(PULL_PATH not in r.body for r in sent)
    # The outcome carries the reminder that went out, so the job's log prints the same text.
    assert [o.reminder for o in outcomes if o.reminder is not None] == sent
    assert all(o.pull is pulled for o in outcomes)


@pytest.mark.parametrize("quiet", [token_store.WROTE, token_store.CURRENT])
def test_a_reminder_says_nothing_of_a_pull_that_left_the_parameter_s_token(fixture_lake, quiet):
    sent = []
    _retry_run(
        _clean_lake(fixture_lake),
        start=SUNDAY_20,
        mints=_Mints(STALE_MINT),
        reminder_sink=sent.append,
        token_pull=_Pulls(_pulled(quiet)),
    )
    assert [r.body for r in sent] == ["The coverage assertion failed. Token minted 2026-08-27."] * 3


def test_a_failed_pull_with_coverage_passing_sends_nothing(fixture_lake):
    # The local token covers the week, so the evening owes no reminder, and the pull's
    # failure reaches only the job's log.
    sent = []
    outcomes, pinger, _ = _retry_run(
        _clean_lake(fixture_lake),
        start=SUNDAY_20,
        mints=_Mints(FRESH_MINT),
        reminder_sink=sent.append,
        token_pull=_Pulls(_pulled(token_store.UNREADABLE, "ParameterNotFound")),
    )
    assert sent == []
    assert len(outcomes) == 1 and outcomes[0].pinged is True
    assert outcomes[0].pull.outcome == token_store.UNREADABLE
    assert pinger.urls == [URL]


def test_a_slow_pull_does_not_move_its_attempt_into_the_next_hour(fixture_lake):
    # Each pull takes 15 minutes. The attempts start at 20:50, 21:20, 21:50, 22:20 and
    # 22:50, so they owe reminders in the 20, 21 and 22 o'clock hours. An attempt that took
    # its moment after the pull would read 21:05, 21:35, 22:05, 22:35 and 23:05, and the
    # 20:00 hour's reminder would never go out.
    root = _clean_lake(fixture_lake)
    clock = ManualClock(start=et(2026, 8, 30, 20, 50))

    def slow_pull():
        clock.sleep(15 * 60)
        return _pulled(token_store.CURRENT)

    sent = []
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
        token_pull=slow_pull,
        reminder_sink=sent.append,
    )
    assert len(outcomes) == 5
    assert len(sent) == 3


def test_a_pull_failing_after_the_hour_s_reminder_is_named_by_the_next_hour_s(fixture_lake):
    # The 20:00 reminder goes out while the pull is still current. The operator re-auths
    # at 20:15 and the pull fails from 20:30 on. The one-an-hour rule drops the 20:30
    # reminder, so that failure reaches only the job's log, and 21:00 names it first.
    failed = _pulled(token_store.UNREADABLE, "the parameter is not JSON")
    sent = []
    outcomes, _, _ = _retry_run(
        _clean_lake(fixture_lake),
        start=SUNDAY_20,
        mints=_Mints(STALE_MINT),
        reminder_sink=sent.append,
        token_pull=_Pulls(_pulled(token_store.CURRENT), failed),
    )
    assert len(outcomes) == 7
    assert outcomes[1].pull is failed and outcomes[1].reminder is None
    assert [r.body for r in sent] == [
        "The coverage assertion failed. Token minted 2026-08-27.",
        "The coverage assertion failed. Token minted 2026-08-27. "
        "Token pull: unreadable (the parameter is not JSON).",
        "The coverage assertion failed. Token minted 2026-08-27. "
        "Token pull: unreadable (the parameter is not JSON).",
    ]


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
    stamped_at=None,
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
        stamped_at=stamped_at,
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
    # Nothing was stamped at all, so the body names the stamp rather than a resident.
    assert cp.NO_PID_NO_STAMP in outcome.daemon_page.body


def test_a_fresh_stamp_with_no_pid_pages_the_daemon_running_old_code(fixture_lake):
    """The Sunday half of the deploy that moved the scheduled job and left the resident.

    ``sunday`` execs fresh every run while the daemon is a resident, so this evening's
    job can read a pid field a daemon from before the pull never writes. The old body
    ended "The machine may idle-sleep before the window closes", which on this machine is
    a wrong diagnosis rather than a vague one: the assertion is held, nothing is going to
    sleep, and the thing to fix is the resident.
    """
    root = _clean_lake(fixture_lake)
    outcome, _ = _daemon_run(
        root,
        daemon_probe=lambda label: True,
        assertion_probe=lambda pid: True,
        assertion_pid=None,
        stamped_at=SUNDAY_20 - timedelta(seconds=30),
    )
    assert outcome.daemon_page is not None
    assert outcome.daemon_page.event == cp.SUNDAY_ASSERTION_UNHELD_EVENT
    assert cp.NO_PID_FRESH_STAMP in outcome.daemon_page.body
    assert "may idle-sleep" not in outcome.daemon_page.body, (
        "a held assertion was reported as a machine about to sleep"
    )


def test_a_stale_stamp_with_no_pid_does_not_page_the_daemon_running_old_code(fixture_lake):
    """The Sunday page needs the freshness bound, not just the ``None`` guard.

    Without a stale case here, the whole freshness decision on this path is unheld by
    anything: swapping the arguments, deleting the comparison, or passing the wall clock
    in place of the stamp all leave the suite green. Each of those tells an operator to
    restart a resident that died hours ago, and says nothing about the stamp that stopped
    moving with it.
    """
    root = _clean_lake(fixture_lake)
    outcome, _ = _daemon_run(
        root,
        daemon_probe=lambda label: True,
        assertion_probe=lambda pid: True,
        assertion_pid=None,
        stamped_at=SUNDAY_20 - cp.STAMP_FRESH_WITHIN - timedelta(seconds=1),
    )
    assert outcome.daemon_page is not None
    assert cp.NO_PID_NO_STAMP in outcome.daemon_page.body
    assert cp.NO_PID_FRESH_STAMP not in outcome.daemon_page.body


def test_a_stamped_pid_that_is_not_held_still_names_the_sleep_risk(fixture_lake):
    """The other half of the page, which the new wording must not swallow.

    A pid that was stamped and is not held really does mean the machine may idle-sleep
    before the window closes. That sentence is right there and stays there, so widening
    the missing-pid case must not reach it.
    """
    root = _clean_lake(fixture_lake)
    outcome, _ = _daemon_run(
        root,
        daemon_probe=lambda label: True,
        assertion_probe=lambda pid: False,
        assertion_pid=_DAEMON_PID,
        stamped_at=SUNDAY_20 - timedelta(seconds=30),
    )
    assert outcome.daemon_page is not None
    assert f"pid {_DAEMON_PID}" in outcome.daemon_page.body
    assert "The machine may idle-sleep before the window closes." in outcome.daemon_page.body
    assert cp.NO_PID_FRESH_STAMP not in outcome.daemon_page.body


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
        token_pull=None,
        daemon_probe=lambda label: False,
        publisher=_publisher(root, transport),
    )
    assert len(outcomes) == 7
    assert all(o.daemon_page is not None for o in outcomes)
    assert len(transport.messages) == 1
    assert transport.messages[0].event == cp.SUNDAY_DAEMON_DOWN_EVENT


def test_the_retry_loop_carries_the_stamp_instant_rather_than_the_wall_clock(fixture_lake):
    """The instant the page reasons about is the stamp's, not the attempt's.

    ``sunday_run`` has the attempt's own moment in hand when it builds the call, so
    handing that over in place of the stamp's instant is a one-word slip that type-checks
    and reads plausibly. It would make every stamp look written this second, so a daemon
    that stopped stamping in March would page as one running old code, every Sunday,
    forever.
    """
    root = _clean_lake(fixture_lake)
    outcomes = cp.sunday_run(
        lake_root=root,
        backup_target=_backup_of(root),
        clock=ManualClock(start=SUNDAY_20),
        calendar=CALENDAR,
        schedule_reader=lambda: REPEAT_ONLY,
        pinger=FakePinger(),
        ping_url=URL,
        mint_reader=_Mints(FRESH_MINT),
        canary=_passing_canary,
        token_pull=None,
        daemon_probe=lambda label: True,
        assertion_probe=lambda pid: True,
        stamp_reader=lambda: JournalMetadata(stamped_at=SUNDAY_20 - timedelta(hours=6)),
    )
    page = outcomes[-1].daemon_page
    assert page is not None
    assert cp.NO_PID_NO_STAMP in page.body
    assert cp.NO_PID_FRESH_STAMP not in page.body, "a six-hour-old stamp read as written now"


def test_the_assertion_pid_is_read_fresh_each_retry(fixture_lake):
    # The daemon restamps a new pid when it re-takes a lost caffeinate mid-evening. A
    # pid cached once at the start of the run would then ask about a child already gone
    # and page a lapse that had already healed, the exact cry-wolf shape the daemon's
    # own re-take exists to avoid. Reading the stamp fresh each attempt, the way the mint
    # already is, is what prevents it.
    root = _clean_lake(fixture_lake)
    mints = _Mints(STALE_MINT)
    # This reader's own call count, rather than the mint reader's. ``sunday_run`` calls
    # the two in one order today and nothing in its contract fixes that order, so a test
    # that borrowed the other counter would break on a reshuffle that changed no
    # behaviour. It did.
    reads = 0

    def read_stamp() -> JournalMetadata:
        # The restamp lands between the first attempt and the second.
        nonlocal reads
        reads += 1
        return JournalMetadata(assertion_pid=4242 if reads <= 1 else 9999)

    def held(pid: int) -> bool:
        # pmset holds an assertion only under the pid currently real. The first
        # child's process is gone once the restamp has happened.
        current = 4242 if reads <= 1 else 9999
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
        token_pull=None,
        daemon_probe=lambda label: True,
        assertion_probe=held,
        stamp_reader=read_stamp,
    )
    assert reads == 7
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
        token_pull=None,
        daemon_probe=lambda label: False,
        # A publisher configured to treat the daemon label as a secret, so the page
        # this run raises is refused the way a page carrying a real one would be.
        publisher=_publisher(root, transport, secrets=(cp.DAEMON_LABEL,)),
    )
    assert transport.messages == []
    err = capsys.readouterr().err
    assert "refused: it carried a secret" in err
    assert cp.DAEMON_LABEL not in err


def test_a_reminder_for_a_failing_canary_names_a_failed_pull_too(fixture_lake):
    # The token covers the week, but the vendor rejects it. The reminder that goes out is
    # the canary's, and a failed pull still rides it, because the fix may be a re-auth put.
    sent = []
    _retry_run(
        _clean_lake(fixture_lake),
        start=SUNDAY_20,
        mints=_Mints(FRESH_MINT),
        canary=lambda: False,
        reminder_sink=sent.append,
        token_pull=_Pulls(_pulled(token_store.UNREADABLE, "ParameterNotFound")),
    )
    assert len(sent) == 3
    assert all(r.body.endswith(" Token pull: unreadable (ParameterNotFound).") for r in sent)


# -- the trimmed ledger --------------------------------------------------------------

# Marketlake #782. The scrub reads ``trimmed.jsonl`` only when it exists, and a ledger it could
# not read reaches the operator as problem lines rather than a raise, because a raise would hide
# every other finding the scrub made that week.

TRIMMED_DAY = date(2026, 8, 28)


def _trim_away(root: Path, fixture_lake: FixtureLake) -> str:
    """Trim the lake's chains partition the way #787 will: the line, its entry, the unlink."""
    from lake.lock import lake_lock
    from lake.manifest import latest_entries
    from lake.trimmed import append_trimmed, trim_line

    path = fixture_lake.partition_path("chains", "SPY", TRIMMED_DAY)
    rel = path.relative_to(root).as_posix()
    line = trim_line(
        rel,
        sha256=latest_entries(root)[rel]["sha256"],
        version_id="v1",
        verified_at="2026-08-29T16:40:00-04:00",
        trimmed_at="2026-08-29T16:41:00-04:00",
    )
    with lake_lock(root):
        append_trimmed(root, line, source="test-trim", fetched_at="2026-08-29T16:41:00-04:00")
    path.unlink()
    return rel


def test_with_no_trimmed_ledger_a_missing_file_renders_the_scrub_line_unchanged(fixture_lake):
    """Mutation this catches: naming the ledger's field on a lake that has no ledger."""
    root = _clean_lake(fixture_lake)
    fixture_lake.partition_path("chains", "SPY", TRIMMED_DAY).unlink()

    outcome, pinger = _run(root)

    assert [p for p in outcome.problems if p.startswith("scrub failed")] == [
        "scrub failed: missing=1 sha_mismatches=0 orphans=0"
    ]
    assert not any("trimmed" in p for p in outcome.problems)
    assert outcome.pinged is False and pinger.urls == []


def test_a_designed_absence_scrubs_clean_and_the_sunday_job_pings(fixture_lake):
    root = _clean_lake(fixture_lake)
    _trim_away(root, fixture_lake)

    outcome, pinger = _run(root)

    assert outcome.scrub.ok, outcome.scrub
    assert outcome.problems == ()
    assert outcome.pinged is True and pinger.urls == [URL]


def test_a_torn_trimmed_ledger_is_two_problem_lines_and_never_a_raise(fixture_lake):
    from lake.manifest import record_partition
    from lake.trimmed import trimmed_path

    root = _clean_lake(fixture_lake)
    rel = _trim_away(root, fixture_lake)
    path = trimmed_path(root)
    path.write_bytes(path.read_bytes() + b'{"kind": "tr\n' + path.read_bytes())
    # Recorded, so the ledger's own bytes are no sha mismatch and the line counts only the tear.
    record_partition(root, "trimmed.jsonl", source="test-trim", rows=3, fetched_at=None)

    outcome, pinger = _run(root)

    assert outcome.scrub.missing == (rel,), "every absence counts as missing"
    assert "scrub failed: missing=1 sha_mismatches=0 orphans=0 trimmed_ledger=unreadable" in (
        outcome.problems
    )
    named = [p for p in outcome.problems if p.startswith("trimmed ledger unreadable")]
    assert len(named) == 1
    assert named[0].startswith(
        "trimmed ledger unreadable, so every absent file counts as missing: TornLedger: "
    )
    assert "trimmed on purpose" in named[0]
    assert outcome.pinged is False and pinger.urls == []


# -- a lake path the scrub cannot read -----------------------------------------------

# Marketlake #441. A path the scrub cannot read is a named finding rather than a raise, because
# every other Sunday check runs after the scrub. Each finding withholds the ping, is named in
# ``report`` by its path, and leaves the canary and the coverage assertion running. The cases
# that come back clean or ``missing`` sit with the scrub's own tests in
# ``tests/component/test_manifest.py``.

_no_root_chmod = pytest.mark.skipif(
    os.geteuid() == 0, reason="root reads and lists past every permission bit"
)


@contextmanager
def _mode(path: Path, mode: int):
    """Hold ``path`` at ``mode`` for the block, and restore its mode whatever happens."""
    original = stat.S_IMODE(path.stat().st_mode)
    path.chmod(mode)
    try:
        yield
    finally:
        path.chmod(original)


def _withheld_and_still_ran(outcome, pinger) -> None:
    """The ping is withheld, and the canary and the coverage assertion ran all the same."""
    assert outcome.pinged is False and pinger.urls == []
    assert outcome.canary_passed is True and outcome.covered is True


@_no_root_chmod
def test_an_unlistable_directory_names_itself_and_the_manifested_file_inside(fixture_lake):
    root = _clean_lake(fixture_lake)
    with _mode(root / "chains" / "ticker=SPY", 0):
        outcome, pinger = _run(root)

    # Two facts. The directory stands for orphans nobody could look for, and the file is a
    # manifested partition the forward pass could not reach.
    assert outcome.scrub.unreadable == (
        f"{CHAINS}: PermissionError",
        "chains/ticker=SPY: PermissionError",
    )
    assert outcome.scrub.missing == ()
    assert "scrub failed: missing=0 sha_mismatches=0 orphans=0 unreadable=2" in outcome.problems
    assert f"lake path could not be read: {CHAINS}: PermissionError" in outcome.report
    assert "lake path could not be read: chains/ticker=SPY: PermissionError" in outcome.report
    _withheld_and_still_ran(outcome, pinger)


@_no_root_chmod
def test_an_unsearchable_directory_names_the_manifested_file_once(fixture_lake):
    # At 0444 the directory lists, and both passes would raise on the file inside it. The
    # reverse pass skips a manifested path before it stats anything, so the file is named once.
    root = _clean_lake(fixture_lake)
    with _mode(root / "chains" / "ticker=SPY", 0o444):
        outcome, pinger = _run(root)

    assert outcome.scrub.unreadable == (f"{CHAINS}: PermissionError",)
    assert outcome.scrub.missing == ()
    assert "scrub failed: missing=0 sha_mismatches=0 orphans=0 unreadable=1" in outcome.problems
    assert f"lake path could not be read: {CHAINS}: PermissionError" in outcome.report
    _withheld_and_still_ran(outcome, pinger)


def test_a_directory_at_a_manifested_path_is_not_a_regular_file(fixture_lake):
    root = _clean_lake(fixture_lake)
    (root / QUOTES).unlink()
    (root / QUOTES).mkdir()

    outcome, pinger = _run(root)

    assert outcome.scrub.unreadable == (f"{QUOTES}: not a regular file",)
    assert outcome.scrub.missing == () and outcome.scrub.sha_mismatches == ()
    assert f"lake path could not be read: {QUOTES}: not a regular file" in outcome.report
    _withheld_and_still_ran(outcome, pinger)


@_no_root_chmod
def test_a_manifested_file_with_no_read_permission_is_unreadable_not_missing(fixture_lake):
    root = _clean_lake(fixture_lake)
    with _mode(root / CHAINS, 0):
        outcome, pinger = _run(root)

    assert outcome.scrub.unreadable == (f"{CHAINS}: PermissionError",)
    assert outcome.scrub.missing == ()
    assert f"lake path could not be read: {CHAINS}: PermissionError" in outcome.report
    _withheld_and_still_ran(outcome, pinger)


def test_a_directory_at_the_trimmed_ledger_is_three_lines(fixture_lake):
    # One fault, two consequences. The ledger cannot be read, so every trimmed partition counts
    # as missing, and the ledger's own manifest entry names a path that is not a file.
    from lake.trimmed import trimmed_path

    root = _clean_lake(fixture_lake)
    rel = _trim_away(root, fixture_lake)
    trimmed_path(root).unlink()
    trimmed_path(root).mkdir()

    outcome, pinger = _run(root)

    assert outcome.scrub.trimmed_unreadable == "not a regular file"
    assert outcome.scrub.unreadable == ("trimmed.jsonl: not a regular file",)
    assert outcome.scrub.missing == (rel,)
    lake_problems = [p for p in outcome.problems if not p.startswith(("backup", "restore"))]
    assert lake_problems == [
        "scrub failed: missing=1 sha_mismatches=0 orphans=0 unreadable=1 trimmed_ledger=unreadable",
        "trimmed ledger unreadable, so every absent file counts as missing: not a regular file",
    ]
    assert [line for line in outcome.report if line.startswith("lake")] == [
        f"lake file missing: {rel}",
        "lake path could not be read: trimmed.jsonl: not a regular file",
    ]
    _withheld_and_still_ran(outcome, pinger)


def test_a_directory_at_the_quarantine_ledger_is_not_a_regular_file(fixture_lake):
    from lake.manifest import append_manifest

    root = _clean_lake(fixture_lake)
    append_manifest(
        root, partition="quarantine.jsonl", source="battery", sha256="s", rows=0, fetched_at=None
    )
    (root / "quarantine.jsonl").mkdir()

    outcome, pinger = _run(root)

    assert outcome.scrub.unreadable == ("quarantine.jsonl: not a regular file",)
    assert outcome.scrub.trimmed_unreadable is None
    assert "scrub failed: missing=0 sha_mismatches=0 orphans=0 unreadable=1" in outcome.problems
    assert "lake path could not be read: quarantine.jsonl: not a regular file" in outcome.report
    _withheld_and_still_ran(outcome, pinger)


@_no_root_chmod
@pytest.mark.parametrize(
    ("mode", "named"),
    [
        pytest.param(0o000, "bars: PermissionError", id="000"),
        pytest.param(0o444, "bars/orphan.parquet: PermissionError", id="0444"),
    ],
)
def test_an_orphan_only_subtree_the_walk_cannot_read_is_named(fixture_lake, mode, named):
    # ``rglob`` dropped a mode-000 directory and reported nothing, so this lake scrubbed clean
    # and pinged. At 0444 it raised from ``is_file()`` instead.
    root = _clean_lake(fixture_lake)
    orphan = root / "bars" / "orphan.parquet"
    orphan.parent.mkdir()
    orphan.write_bytes(b"written without its entry")
    with _mode(orphan.parent, mode):
        outcome, pinger = _run(root)

    assert outcome.scrub.unreadable == (named,)
    assert outcome.scrub.orphans == ()
    assert f"lake path could not be read: {named}" in outcome.report
    _withheld_and_still_ran(outcome, pinger)


def test_a_fifo_at_a_manifested_path_is_named_without_being_opened(fixture_lake):
    root = _clean_lake(fixture_lake)
    fifo = root / QUOTES
    fifo.unlink()
    os.mkfifo(fifo)

    outcome, pinger = without_blocking(fifo, lambda: _run(root))

    assert outcome.scrub.unreadable == (f"{QUOTES}: not a regular file",)
    assert f"lake path could not be read: {QUOTES}: not a regular file" in outcome.report
    _withheld_and_still_ran(outcome, pinger)


def test_a_fifo_at_the_trimmed_ledger_is_named_without_being_opened(fixture_lake):
    from lake.trimmed import trimmed_path

    root = _clean_lake(fixture_lake)
    rel = _trim_away(root, fixture_lake)
    fifo = trimmed_path(root)
    fifo.unlink()
    os.mkfifo(fifo)

    outcome, pinger = without_blocking(fifo, lambda: _run(root))

    assert outcome.scrub.trimmed_unreadable == "not a regular file"
    assert outcome.scrub.unreadable == ("trimmed.jsonl: not a regular file",)
    assert outcome.scrub.missing == (rel,)
    _withheld_and_still_ran(outcome, pinger)


@_no_root_chmod
def test_a_nested_lost_and_found_is_walked_and_named(fixture_lake):
    # Only the root's ``lost+found`` is the filesystem's. One deeper is the lake's.
    root = _clean_lake(fixture_lake)
    nested = root / "chains" / "lost+found"
    nested.mkdir()
    with _mode(nested, 0):
        outcome, pinger = _run(root)

    assert outcome.scrub.unreadable == ("chains/lost+found: PermissionError",)
    assert "lake path could not be read: chains/lost+found: PermissionError" in outcome.report
    _withheld_and_still_ran(outcome, pinger)


def test_a_read_failing_another_way_is_named_by_its_class(fixture_lake, monkeypatch):
    # Every permission case raises ``PermissionError``, so only this one fails a catch narrowed
    # to it. An ``EIO`` that reached the guard would hide every other finding.
    from lake import manifest

    root = _clean_lake(fixture_lake)
    real = manifest.sha256_file

    def failing(path: Path) -> str:
        if root in Path(path).parents:
            raise OSError(errno.EIO, "Input/output error", str(path))
        return real(path)

    monkeypatch.setattr(manifest, "sha256_file", failing)

    outcome, pinger = _run(root)

    assert outcome.scrub.unreadable == (f"{CHAINS}: OSError", f"{QUOTES}: OSError")
    assert f"lake path could not be read: {CHAINS}: OSError" in outcome.report
    _withheld_and_still_ran(outcome, pinger)


def test_every_lake_finding_is_named_in_order_beside_the_backup_notes(fixture_lake):
    from lake.manifest import append_manifest

    day = date(2026, 8, 28)
    fixture_lake.with_chains("SPY", day).with_quotes("SPY", day).with_chains("QQQ", day)
    root = fixture_lake.build()
    mirror_lake(root, _backup_of(root))
    gone_from_lake = "chains/ticker=QQQ/date=2026-08-28.parquet"
    (root / gone_from_lake).unlink()
    (root / CHAINS).write_bytes(b"rot")
    (root / QUOTES).unlink()
    (_backup_of(root) / QUOTES).unlink()
    (root / "bars").mkdir()
    (root / "bars" / "orphan.parquet").write_bytes(b"written without its entry")
    # Manifested in reverse order, so the forward pass meets them out of order and only the
    # sort puts them right. Appended after the copy, so the backup counts them as pending.
    for partition in ("zz/inner.parquet", "aa/inner.parquet"):
        append_manifest(
            root, partition=partition, source="capture", sha256="s", rows=1, fetched_at=None
        )
        (root / partition).mkdir(parents=True)

    outcome, pinger = _run(root, schedule="")

    alarm_lines = outcome.alarms.problems
    assert alarm_lines
    assert outcome.report == (
        *alarm_lines,
        f"lake file missing: {gone_from_lake}",
        f"lake file missing: {QUOTES}",
        f"lake file does not match its sha: {CHAINS}",
        "lake file has no manifest entry: bars/orphan.parquet",
        "lake path could not be read: aa/inner.parquet: not a regular file",
        "lake path could not be read: zz/inner.parquet: not a regular file",
        f"backup file gone: {QUOTES}",
        "backup behind the lake by 2 partitions",
    )
    assert "scrub failed: missing=2 sha_mismatches=1 orphans=1 unreadable=2" in outcome.problems
    _withheld_and_still_ran(outcome, pinger)


# -- a scrub that raises -------------------------------------------------------------

# A fault that leaves the scrub nothing to say per path still raises from it. Each of the
# three scrub calls is guarded, so the raise becomes a problem line and the rest of the run
# goes on. The damage is made before the copy, so the copy carries the same bytes and the
# path scrub reaches the line. Damage that rewrites bytes the copy already holds stops the
# path scrub at the diverged manifest before it parses a line, and a test of the backup
# guard would then pass with the guard deleted. A damaged line appended after a clean copy
# is different: the copy stays a strict prefix, the scrub resolves the whole lake manifest,
# and the guard fires.


def _damaged_then_copied(fixture_lake: FixtureLake, old: bytes, new: bytes) -> Path:
    """A clean two-partition lake whose manifest has ``old`` replaced once, then copied."""
    fixture_lake.with_chains("SPY", date(2026, 8, 28))
    fixture_lake.with_quotes("SPY", date(2026, 8, 28))
    root = fixture_lake.build()
    path = root / "manifest.jsonl"
    raw = path.read_bytes()
    assert old in raw
    path.write_bytes(raw.replace(old, new, 1))
    mirror_lake(root, _backup_of(root))
    return root


def _tracebacks_printed(err: str) -> int:
    """How many stack traces ``err`` holds, counting a chained cause as part of its trace."""
    chained = err.count("The above exception was the direct cause") + err.count(
        "During handling of the above exception"
    )
    return err.count("Traceback (most recent call last)") - chained


def _manifest_named(root: Path) -> str:
    return str(root / "manifest.jsonl")


@pytest.mark.parametrize(
    ("old", "new", "lake", "backup"),
    [
        pytest.param(
            b"capture",
            b"captur\xff",
            lambda root: "lake scrub could not run: LedgerNotUtf8: ",
            lambda root: [],
            id="not utf-8",
        ),
        pytest.param(
            b'"partition"',
            b'"partitioX"',
            lambda root: (
                f"lake scrub could not run: ManifestError: {_manifest_named(root)}: entry 1 "
                "names no partition"
            ),
            lambda root: [
                "backup could not be read: the backup scrub raised ManifestError: "
                f"{_manifest_named(root)}: entry 1 names no partition"
            ],
            id="no partition",
        ),
        pytest.param(
            b'"sha256"',
            b'"sha25X"',
            lambda root: "lake scrub could not run: KeyError: 'sha256'",
            lambda root: ["backup could not be read: the backup scrub raised KeyError: 'sha256'"],
            id="no sha256",
        ),
    ],
)
def test_a_damaged_lake_manifest_is_a_problem_line_on_a_path_target(
    fixture_lake, capsys, old, new, lake, backup
):
    root = _damaged_then_copied(fixture_lake, old, new)

    outcome, pinger = _run(root)

    assert outcome.scrub is None
    lake_line, *backup_lines = outcome.problems
    assert lake_line.startswith(lake(root))
    assert backup_lines == backup(root)
    if backup_lines:
        # The guard's result is the backup's own unreadable finding: no restore test, and not
        # the shadow host's skip line.
        assert outcome.backup.unreadable is not None and outcome.restore is None
    else:
        assert outcome.backup.ok and outcome.restore is not None
    assert cp.BACKUP_SCRUB_SKIPPED not in outcome.report
    # Each guard that fired kept the stack trace, so a bug in a scrub is not lost to the line.
    assert _tracebacks_printed(capsys.readouterr().err) == 1 + len(backup_lines)
    _withheld_and_still_ran(outcome, pinger)


@_no_root_chmod
def test_an_unreadable_lake_manifest_is_named_by_each_scrub(fixture_lake):
    # Made after the copy, since the copy refuses an unreadable file. The backup scrub names
    # it through its own ``except OSError``.
    root = _clean_lake(fixture_lake)
    with _mode(root / "manifest.jsonl", 0):
        outcome, pinger = _run(root)

    assert outcome.scrub is None
    lake_line, backup_line = outcome.problems
    assert lake_line.startswith("lake scrub could not run: PermissionError: ")
    assert backup_line.startswith("backup could not be read: PermissionError: ")
    assert outcome.restore is None
    _withheld_and_still_ran(outcome, pinger)


@_no_root_chmod
def test_a_lake_root_whose_parent_cannot_be_searched_is_a_problem_not_a_raise(tmp_path):
    lake = FixtureLake(tmp_path / "locked" / "lake")
    lake.with_chains("SPY", date(2026, 8, 28))
    root = lake.build()
    backup = mirror_lake(root, tmp_path / "ssd")
    pinger = FakePinger()
    with _mode(tmp_path / "locked", 0):
        outcome = cp.sunday_maintenance(
            lake_root=root,
            backup_target=backup,
            now=SUNDAY_20,
            calendar=CALENDAR,
            schedule_reader=lambda: REPEAT_ONLY,
            pinger=pinger,
            ping_url=URL,
            mint=FRESH_MINT,
            canary=_passing_canary,
        )

    assert outcome.scrub is None
    lake_line, backup_line = outcome.problems
    assert lake_line.startswith("lake scrub could not run: PermissionError: ")
    assert backup_line.startswith("backup could not be read: PermissionError: ")
    _withheld_and_still_ran(outcome, pinger)


# -- a backup path that is not a regular file ----------------------------------------

# ``rsync -a`` copies a FIFO as a FIFO, so the copy can hold one the lake scrub has already
# named. Reading it would block the job, so the backup scrub records it per file and goes on.


def test_a_fifo_on_the_copy_is_named_without_being_opened_and_the_walk_finishes(fixture_lake):
    root = _clean_lake(fixture_lake)
    fifo = _backup_of(root) / QUOTES
    fifo.unlink()
    os.mkfifo(fifo)

    outcome, pinger = without_blocking(fifo, lambda: _run(root))

    assert outcome.backup.not_regular == (QUOTES,)
    assert outcome.backup.walked is True
    # The restore test ran, on the one file the scrub matched.
    assert outcome.restore is not None and outcome.restore.restored == (CHAINS,)
    assert outcome.problems == (
        "backup scrub failed: missing=0 sha_mismatches=0 unaccounted=0 not_regular=1: "
        f"{_backup_of(root)}",
    )
    assert (
        f"backup path is not a regular file, so remove it from the copy first: {QUOTES}"
        in outcome.report
    )
    _withheld_and_still_ran(outcome, pinger)


def test_a_directory_on_the_copy_is_not_a_regular_file_and_the_walk_finishes(fixture_lake):
    # A check narrowed to FIFOs would send this to the stopping ``IsADirectoryError`` branch.
    root = _clean_lake(fixture_lake)
    (_backup_of(root) / QUOTES).unlink()
    (_backup_of(root) / QUOTES).mkdir()

    outcome, pinger = _run(root)

    assert outcome.backup.not_regular == (QUOTES,)
    assert outcome.backup.walked is True and outcome.backup.unreadable is None
    assert outcome.restore is not None
    _withheld_and_still_ran(outcome, pinger)
