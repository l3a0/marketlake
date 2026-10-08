"""The range restore, ``python -m lake.bucket restore-range``, against the fake S3 client.

Marketlake #784. The command puts chosen chains or quotes partitions back into the live lake,
for a partition trimmed on purpose or one lost by accident. Each test builds a lake, stores its
partitions in the fake bucket the way the nightly upload would, removes some, and restores them.
The cases follow the issue's Verification list.

1. The commit's order: a crash after each step leaves a state the next run repairs, and never
   an absence nothing explains.
2. Repair 1: a selected present file whose latest trimmed line is a trim line gets its restore
   line, and the same state outside the selection is left for the next trim.
3. Repair 2, the ledger's entry, is in ``tests/component/test_trimmed.py`` beside the ledger.
4. A lost partition comes back with no restore line.
5. A leftover temp file is removed, and no ``.part`` file is ever left in the lake.
6. The refusals: the Sunday window, a session, a present file that differs, a surface other
   than chains or quotes, a lake walk that cannot read every path, and a shadow host.
7. The reserve's arithmetic is in ``tests/unit/test_runway.py``. A component case here proves
   the command refuses on it.
8. A bucket version that cannot serve the partition names the trim line's ``version_id``.

Expected bytes and digests are read from the lake before anything is removed, never through
the code under test.
"""

from __future__ import annotations

import hashlib
import os
from datetime import date, datetime
from pathlib import Path

import pytest

from lake import bucket, trimmed
from lake.bucket import RangeRestoreRefused, restore_range
from lake.calendar import MARKET_TZ
from lake.config import BucketTarget
from lake.lock import lake_lock
from lake.manifest import append_manifest, latest_entries, scrub, sha256_file
from lake.trimmed import append_trimmed, latest_trimmed, trim_line, trimmed_path
from tests.support.bucket import FakeS3
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.lake import FixtureLake

TARGET = BucketTarget(bucket="lake-backup", prefix="lake")
D1 = date(2026, 8, 24)
D2 = date(2026, 8, 25)
D3 = date(2026, 8, 26)
SPY_1 = "chains/ticker=SPY/date=2026-08-24.parquet"
SPY_2 = "chains/ticker=SPY/date=2026-08-25.parquet"
SPY_3 = "chains/ticker=SPY/date=2026-08-26.parquet"
QQQ_1 = "chains/ticker=QQQ/date=2026-08-24.parquet"
QUOTES_1 = "quotes/ticker=SPY/date=2026-08-24.parquet"
# A Monday evening after the sweep, a Sunday inside the scrub window, and a Monday inside the
# session. The next session after the Monday evening opens on Tuesday.
MONDAY_19 = datetime(2026, 8, 31, 19, 0, tzinfo=MARKET_TZ)
SUNDAY_20 = datetime(2026, 8, 30, 20, 0, tzinfo=MARKET_TZ)
MONDAY_10 = datetime(2026, 8, 31, 10, 0, tzinfo=MARKET_TZ)
# Monday's open at 09:30, less the 15-minute in-flight allowance and the 15-minute pre-open
# margin, written out rather than computed from the constants under test.
MONDAY_BOUND = datetime(2026, 8, 31, 9, 0, tzinfo=MARKET_TZ)
CALENDAR = weekday_sessions(date(2026, 8, 24), date(2026, 8, 31))
PLENTY = 10**12
STAMP = "2026-08-28T19:00:00-04:00"
KEYS = (
    "bucket_access_key_id: AKIDCONFIG\n"
    "bucket_secret_access_key: secret-bucket-key\n"
    "bucket_region: us-east-2\n"
)


class Crash(Exception):
    """A process death injected between two steps. No branch of the command catches it."""


def _lake(tmp_path: Path) -> tuple[Path, FakeS3, dict[str, bytes]]:
    """A lake of five partitions, each stored in the fake bucket as the nightly upload would."""
    root = (
        FixtureLake(tmp_path / "lake")
        .with_chains("SPY", D1)
        .with_chains("SPY", D2)
        .with_chains("SPY", D3)
        .with_chains("QQQ", D1)
        .with_quotes("SPY", D1)
        .build()
    )
    client = FakeS3()
    originals: dict[str, bytes] = {}
    for rel in (SPY_1, SPY_2, SPY_3, QQQ_1, QUOTES_1):
        data = (root / rel).read_bytes()
        originals[rel] = data
        client.store(TARGET.key(rel), data)
    return root, client, originals


def _trim_away(root: Path, client: FakeS3, rel: str, *, unlink: bool = True) -> str:
    """Trim a partition the way marketlake #787 will: the trim line, then the unlink.

    It returns the bucket version the trim line records. ``unlink=False`` leaves the file, which
    is what a trim that crashed between its line and its unlink leaves.
    """
    version = client.versions(TARGET.key(rel))[-1].version_id
    line = trim_line(
        rel,
        sha256=latest_entries(root)[rel]["sha256"],
        version_id=version,
        verified_at=STAMP,
        trimmed_at=STAMP,
    )
    with lake_lock(root):
        append_trimmed(root, line, source="test-trim", fetched_at=STAMP)
    if unlink:
        (root / rel).unlink()
    return version


def _run(
    root: Path,
    client: FakeS3,
    *,
    ticker: str | None = "SPY",
    first: date = D1,
    last: date = D3,
    surface: str = "chains",
    now: datetime = MONDAY_19,
    free: int = PLENTY,
) -> bucket.RangeRestoreSummary:
    return restore_range(
        root,
        TARGET,
        client=client,
        clock=ManualClock(now),
        calendar=CALENDAR,
        surface=surface,
        ticker=ticker,
        first=first,
        last=last,
        free_space=lambda path: free,
    )


def _kind(root: Path, rel: str) -> str | None:
    line = latest_trimmed(root).get(rel)
    return None if line is None else line["kind"]


def _strays(root: Path) -> list[str]:
    """Every in-flight file left anywhere under the lake root, ``.part`` or temp."""
    return sorted(
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.name.endswith(".part") or ".tmp-" in path.name
    )


def _unexplained(root: Path) -> tuple[str, ...]:
    """What the Sunday scrub would call missing: an absence nothing explains."""
    return scrub(root).missing


# -- 1. the round trip and the order of the commit ------------------------------


def test_a_designed_absence_comes_back_with_its_restore_line_and_a_matching_entry(tmp_path):
    root, client, originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1)
    _trim_away(root, client, SPY_2)

    summary = _run(root, client)

    assert (root / SPY_1).read_bytes() == originals[SPY_1]
    assert (root / SPY_2).read_bytes() == originals[SPY_2]
    assert (summary.restored, summary.present, summary.restore_lines) == (2, 1, 2)
    assert summary.selected == 3
    assert _kind(root, SPY_1) == "restore" and _kind(root, SPY_2) == "restore"
    line = latest_trimmed(root)[SPY_1]
    assert line["sha256"] == hashlib.sha256(originals[SPY_1]).hexdigest()
    assert latest_entries(root)["trimmed.jsonl"]["sha256"] == sha256_file(trimmed_path(root))
    assert latest_entries(root)["trimmed.jsonl"]["source"] == bucket.RANGE_RESTORE_SOURCE
    assert scrub(root).ok
    assert _strays(root) == []


def test_a_second_run_after_success_writes_nothing(tmp_path):
    root, client, _originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1)
    _run(root, client)
    ledger = trimmed_path(root).read_bytes()
    manifest = (root / "manifest.jsonl").read_bytes()

    summary = _run(root, client)

    assert (summary.restored, summary.present, summary.restore_lines) == (0, 3, 0)
    assert not summary.ledger_repaired
    assert trimmed_path(root).read_bytes() == ledger
    assert (root / "manifest.jsonl").read_bytes() == manifest


def test_a_crash_before_the_move_leaves_the_absence_explained_and_the_rerun_restores(
    tmp_path, monkeypatch
):
    """Mutation this catches: appending the restore line before ``os.replace``.

    A restore line ahead of the file makes the partition read as lost, since the line
    supersedes the trim line that explained the absence.
    """
    root, client, originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1)

    def crash(source, destination):
        raise Crash("killed before the rename")

    monkeypatch.setattr(bucket.os, "replace", crash)
    with pytest.raises(Crash):
        _run(root, client, last=D1)
    monkeypatch.undo()

    assert not (root / SPY_1).exists()
    assert _unexplained(root) == ()
    assert _kind(root, SPY_1) == "trim"

    summary = _run(root, client, last=D1)

    assert (root / SPY_1).read_bytes() == originals[SPY_1]
    assert summary.restored == 1 and _kind(root, SPY_1) == "restore"
    assert scrub(root).ok


def test_a_crash_after_the_move_and_before_the_line_is_repaired_by_the_rerun(tmp_path, monkeypatch):
    root, client, originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1)

    def crash(*args, **kwargs):
        raise Crash("killed before the restore line")

    monkeypatch.setattr(trimmed, "append_trimmed", crash)
    with pytest.raises(Crash):
        _run(root, client, last=D1)
    monkeypatch.undo()

    assert (root / SPY_1).read_bytes() == originals[SPY_1]
    assert _kind(root, SPY_1) == "trim"
    assert _unexplained(root) == ()

    summary = _run(root, client, last=D1)

    assert (summary.restored, summary.present, summary.restore_lines) == (0, 1, 1)
    assert _kind(root, SPY_1) == "restore"
    assert scrub(root).ok


def test_a_crash_between_the_line_and_its_entry_is_repaired_by_the_rerun(tmp_path, monkeypatch):
    root, client, _originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1)

    def crash(*args, **kwargs):
        raise Crash("killed between the append and the record")

    monkeypatch.setattr(trimmed, "_record", crash)
    with pytest.raises(Crash):
        _run(root, client, last=D1)
    monkeypatch.undo()

    assert _kind(root, SPY_1) == "restore"
    assert latest_entries(root)["trimmed.jsonl"]["sha256"] != sha256_file(trimmed_path(root))
    assert _unexplained(root) == ()

    summary = _run(root, client, last=D1)

    assert summary.ledger_repaired
    assert summary.restore_lines == 0
    assert latest_entries(root)["trimmed.jsonl"]["sha256"] == sha256_file(trimmed_path(root))
    assert scrub(root).ok


# -- 2. repair 1 stays inside the selection --------------------------------------


def test_a_present_file_under_a_trim_line_gets_its_restore_line_only_inside_the_selection(
    tmp_path,
):
    """Mutations this catches: skipping repair 1, and widening it to the whole lake.

    The same state outside the selection is what a trim that crashed before its unlink leaves,
    and the next trim finishes that unlink, so a restore line there would cancel a trim.
    """
    root, client, _originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1, unlink=False)
    _trim_away(root, client, SPY_2, unlink=False)

    summary = _run(root, client, last=D1)

    assert summary.restore_lines == 1 and summary.restored == 0
    assert _kind(root, SPY_1) == "restore"
    assert _kind(root, SPY_2) == "trim"


@pytest.mark.parametrize("race", ["reentered", "resealed"])
def test_a_lake_that_moved_during_the_download_refuses_the_commit_and_keeps_no_temp(
    tmp_path, monkeypatch, race
):
    """The download runs unlocked, so the commit re-checks under the lock.

    Mutations this catches: dropping that re-check, which overwrites a file another writer
    put there or installs bytes the manifest no longer records, and leaving the temp behind
    when the commit refuses, which the Sunday scrub reads as an orphan.
    """
    root, client, originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    download = bucket._download_to

    def racing(read, rel, part):
        result = download(read, rel, part)
        if race == "reentered":
            (root / SPY_1).write_bytes(b"another writer's bytes")
        else:
            with lake_lock(root):
                append_manifest(
                    root,
                    partition=SPY_1,
                    source="recompact",
                    sha256="f" * 64,
                    rows=10**6,
                    fetched_at=STAMP,
                )
        return result

    monkeypatch.setattr(bucket, "_download_to", racing)

    with pytest.raises(RangeRestoreRefused, match="changed in the lake while it downloaded"):
        _run(root, client, last=D1)

    assert _strays(root) == []
    if race == "reentered":
        assert (root / SPY_1).read_bytes() == b"another writer's bytes"
    else:
        assert not (root / SPY_1).exists()
    assert originals[SPY_1] != b"another writer's bytes"


# -- 4. a lost partition ----------------------------------------------------------


def test_a_lost_partition_comes_back_with_no_restore_line(tmp_path):
    """Mutation this catches: writing a restore line unconditionally."""
    root, client, originals = _lake(tmp_path)
    (root / SPY_2).unlink()
    assert _unexplained(root) == (SPY_2,)

    summary = _run(root, client)

    assert (root / SPY_2).read_bytes() == originals[SPY_2]
    assert summary.restored == 1 and summary.restore_lines == 0
    assert not trimmed_path(root).exists()
    assert scrub(root).ok


def test_every_ticker_is_selected_when_none_is_named(tmp_path):
    root, client, originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    (root / QQQ_1).unlink()

    summary = _run(root, client, ticker=None, last=D1)

    assert summary.selected == 2 and summary.restored == 2
    assert (root / QQQ_1).read_bytes() == originals[QQQ_1]


def test_a_quotes_partition_is_restored_and_its_chains_twin_is_not_selected(tmp_path):
    root, client, originals = _lake(tmp_path)
    (root / QUOTES_1).unlink()
    (root / SPY_1).unlink()

    summary = _run(root, client, surface="quotes", last=D1)

    assert summary.selected == 1
    assert (root / QUOTES_1).read_bytes() == originals[QUOTES_1]
    assert not (root / SPY_1).exists()


# -- 5. temp files ----------------------------------------------------------------


def test_a_leftover_temp_is_removed_and_no_part_file_is_ever_left(tmp_path, monkeypatch):
    """Mutation this catches: reverting ``_download_to`` to ``.part`` for this caller.

    A ``.part`` file sits in neither exclusion list, so the nightly upload would send it and
    the Sunday scrub would call it an orphan, and the rerun's temp sweep never finds it.
    """
    root, client, _originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1)

    def crash(source, destination):
        raise Crash("killed before the rename")

    monkeypatch.setattr(bucket.os, "replace", crash)
    with pytest.raises(Crash):
        _run(root, client, last=D1)
    monkeypatch.undo()
    left = _strays(root)
    assert len(left) == 1 and ".tmp-" in left[0]
    assert left[0].startswith("chains/ticker=SPY/date=2026-08-24.parquet")

    summary = _run(root, client, last=D1)

    assert summary.temps_removed == left
    assert _strays(root) == []
    assert scrub(root).ok


def test_a_temp_beside_a_target_from_another_dead_writer_is_removed(tmp_path):
    root, client, _originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    stray = root / "chains/ticker=SPY/date=2026-08-24.parquet.tmp-99999"
    stray.write_bytes(b"half a partition")
    unrelated = root / "chains/ticker=SPY/notes.tmp-1"
    unrelated.write_bytes(b"not beside a target")

    summary = _run(root, client, last=D1)

    assert summary.temps_removed == ["chains/ticker=SPY/date=2026-08-24.parquet.tmp-99999"]
    assert not stray.exists()
    assert unrelated.exists()


# -- 6. refusals -------------------------------------------------------------------


def _refuses(root: Path, client: FakeS3, **kwargs) -> str:
    manifest = (root / "manifest.jsonl").read_bytes()
    with pytest.raises(RangeRestoreRefused) as exc:
        _run(root, client, **kwargs)
    assert (root / "manifest.jsonl").read_bytes() == manifest
    return str(exc.value)


def test_the_sunday_scrub_window_refuses_before_any_request(tmp_path):
    root, client, _originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    client.calls.clear()

    line = _refuses(root, client, now=SUNDAY_20)

    assert "Sunday" in line and "19:55" in line
    assert client.calls == []
    assert not (root / SPY_1).exists()


@pytest.mark.parametrize("now", [MONDAY_10, MONDAY_BOUND], ids=["in-session", "at-the-bound"])
def test_a_session_refuses_before_any_request(tmp_path, now):
    """The bound itself refuses. Mutation this catches: a strict comparison at the bound."""
    root, client, _originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    client.calls.clear()

    line = _refuses(root, client, now=now)

    assert "next session" in line
    assert client.calls == []
    assert not (root / SPY_1).exists()


def test_a_minute_before_the_bound_still_restores(tmp_path):
    root, client, originals = _lake(tmp_path)
    (root / SPY_1).unlink()

    summary = _run(root, client, now=datetime(2026, 8, 31, 8, 59, tzinfo=MARKET_TZ))

    assert summary.restored == 1
    assert (root / SPY_1).read_bytes() == originals[SPY_1]


def test_a_present_file_that_differs_refuses_and_restores_nothing(tmp_path):
    root, client, _originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    (root / SPY_3).write_bytes(b"rotted bytes")
    client.calls.clear()

    line = _refuses(root, client)

    assert SPY_3 in line and "does not match its manifest entry" in line
    assert client.calls == []
    assert not (root / SPY_1).exists()


@pytest.mark.parametrize("surface", ["bars", "actions"])
def test_a_surface_other_than_chains_or_quotes_refuses(tmp_path, surface):
    root, client, _originals = _lake(tmp_path)
    client.calls.clear()

    line = _refuses(root, client, surface=surface)

    assert repr(surface) in line
    assert client.calls == []


def test_a_selector_matching_nothing_refuses_rather_than_report_success(tmp_path):
    root, client, _originals = _lake(tmp_path)

    line = _refuses(root, client, ticker="SPYY")

    assert "records no chains partitions for SPYY" in line


def test_a_lake_root_with_no_manifest_refuses_and_is_left_without_one(tmp_path):
    root = tmp_path / "not-a-lake"
    root.mkdir()

    with pytest.raises(RangeRestoreRefused, match="holds no manifest.jsonl"):
        _run(root, FakeS3())

    assert not (root / "manifest.jsonl").exists()


def test_a_lake_walk_that_cannot_read_every_path_refuses(tmp_path):
    """Mutation this catches: ignoring the walk's refusals, which fails the reserve open."""
    root, client, _originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    locked = root / "reports"
    locked.mkdir()
    (locked / "date=2026-08-28.md").write_text("a report\n")
    locked.chmod(0)
    try:
        line = _refuses(root, client)
    finally:
        locked.chmod(0o755)

    assert "could not read 1 path(s)" in line
    assert not (root / SPY_1).exists()


def test_free_space_short_of_the_journal_reserve_refuses_and_room_for_it_passes(tmp_path):
    root, client, originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    planned = len(originals[SPY_1])
    from lake import runway

    usage = runway.walk(root)
    busiest = runway.busiest_sealed_day(usage, today=MONDAY_19.date())
    needed = planned + runway.JOURNAL_RESERVE_SESSIONS * busiest

    line = _refuses(root, client, free=needed - 1)

    assert "short of the journal reserve" in line
    assert not (root / SPY_1).exists()
    assert _run(root, client, free=needed).restored == 1


def test_a_shadow_host_refuses_with_one_line(tmp_path, monkeypatch, capsys):
    root, client, _originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    config = write_config(tmp_path, root, role="shadow")
    config.write_text(config.read_text() + KEYS)
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    client.calls.clear()

    code = bucket.main(_argv(config), clock=ManualClock(MONDAY_19), calendar=CALENDAR)

    assert code == 2
    err = capsys.readouterr().err.splitlines()
    assert len(err) == 1 and err[0].startswith("restore-range: ")
    assert client.calls == []
    assert not (root / SPY_1).exists()


def _argv(config: Path) -> list[str]:
    return [
        "restore-range",
        "--surface",
        "chains",
        "--ticker",
        "SPY",
        "--from",
        "2026-08-24",
        "--to",
        "2026-08-26",
        "--config",
        str(config),
        "--target",
        "s3://lake-backup/lake",
    ]


def test_the_command_restores_and_prints_one_line(tmp_path, monkeypatch, capsys):
    root, client, originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    config = write_config(tmp_path, root)
    config.write_text(config.read_text() + KEYS)
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)

    code = bucket.main(_argv(config), clock=ManualClock(MONDAY_19), calendar=CALENDAR)

    assert code == 0
    assert (root / SPY_1).read_bytes() == originals[SPY_1]
    out = capsys.readouterr().out.splitlines()
    assert out == [out[0]] and out[0].startswith("restore-range: restored 1 of 3")


def test_a_refusal_reaches_the_operator_as_one_line_and_exit_2(tmp_path, monkeypatch, capsys):
    root, client, _originals = _lake(tmp_path)
    (root / SPY_3).write_bytes(b"rotted bytes")
    config = write_config(tmp_path, root)
    config.write_text(config.read_text() + KEYS)
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)

    with pytest.raises(SystemExit) as exc:
        bucket.main(_argv(config), clock=ManualClock(MONDAY_19), calendar=CALENDAR)

    assert exc.value.code == 2
    captured = capsys.readouterr()
    lines = captured.err.splitlines()
    assert len(lines) == 1 and lines[0].startswith("restore-range: ")
    assert "secret-bucket-key" not in captured.err


# -- 8. a version the bucket cannot serve ------------------------------------------


def test_a_current_version_that_does_not_match_names_the_trim_lines_version(tmp_path):
    root, client, _originals = _lake(tmp_path)
    version = _trim_away(root, client, SPY_1)
    client.store(TARGET.key(SPY_1), b"an overwrite after the trim")

    line = _refuses(root, client, last=D1)

    assert f"version {version}" in line
    assert "does not match its manifest entry" in line
    assert not (root / SPY_1).exists()
    assert _strays(root) == []
    assert _kind(root, SPY_1) == "trim"


def test_a_partition_the_bucket_does_not_hold_refuses_before_any_download(tmp_path):
    root, client, _originals = _lake(tmp_path)
    version = _trim_away(root, client, SPY_1)
    del client.objects[TARGET.key(SPY_1)]

    line = _refuses(root, client, last=D1)

    assert f"version {version}" in line and "holds no current version" in line
    assert not [kwargs for name, kwargs in client.calls if name == "get_object"]


def test_a_refusal_after_a_commit_says_how_many_were_restored(tmp_path):
    root, client, originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1)
    _trim_away(root, client, SPY_2)
    client.store(TARGET.key(SPY_2), b"an overwrite after the trim")

    line = _refuses_after_commit(root, client)

    assert "1 partition(s) were restored before this stop" in line
    assert (root / SPY_1).read_bytes() == originals[SPY_1]
    assert _kind(root, SPY_1) == "restore"


def _refuses_after_commit(root: Path, client: FakeS3) -> str:
    with pytest.raises(RangeRestoreRefused) as exc:
        _run(root, client)
    return str(exc.value)


def test_the_temp_name_is_the_one_the_backup_excludes(tmp_path):
    """The in-flight name has to be the backup's excluded marker, never a suffix of its own."""
    root, _client, _originals = _lake(tmp_path)
    temp = bucket.temp_write_path(root / SPY_1, os.getpid())

    assert bucket.rsync_excluded(temp.relative_to(root).as_posix(), is_dir=False)
    assert not bucket.rsync_excluded(SPY_1 + ".part", is_dir=False)
