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

import errno
import hashlib
import os
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path

import pytest

from lake import bucket, trimmed
from lake.bucket import RangeRestoreRefused, restore_range
from lake.calendar import MARKET_TZ
from lake.config import BucketTarget
from lake.lock import lake_lock
from lake.manifest import ManifestError, append_manifest, latest_entries, scrub, sha256_file
from lake.trimmed import (
    TrimmedLineLost,
    append_trimmed,
    latest_trimmed,
    read_trimmed,
    restore_line,
    trim_line,
    trimmed_path,
)
from tests.support.bucket import FakeS3, client_error
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


def _watch_downloads(
    monkeypatch, root: Path, seen: list[str], stop: BaseException | None = None
) -> None:
    """Wrap the bucket reader so each download records its target directory mid-flight.

    The listing is taken after the first chunk is handed over, while the in-flight file is
    open. With ``stop`` set, the read then raises it, the way an interrupt or a client bug
    would partway through a body.
    """
    real = bucket.bucket_reader

    def reader(client, target):
        read = real(client, target)

        def watched(rel):
            chunks = read(rel)
            first = next(chunks)
            yield first
            seen.extend(sorted(os.listdir((root / rel).parent)))
            if stop is not None:
                raise stop
            yield from chunks

        return watched

    monkeypatch.setattr(bucket, "bucket_reader", reader)


def test_a_download_in_flight_carries_the_name_the_backup_excludes(tmp_path, monkeypatch):
    """Mutation this catches: reverting ``_download_to`` to ``.part`` for this caller.

    A ``.part`` file sits in neither exclusion list, so the nightly upload would send one in
    flight, the Sunday scrub would call a leftover an orphan, and the next run's sweep of
    ``<name>.tmp-<pid>`` would never find it.
    """
    root, client, originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    seen: list[str] = []
    _watch_downloads(monkeypatch, root, seen)

    _run(root, client, last=D1)

    in_flight = [name for name in seen if name.startswith("date=2026-08-24.parquet")]
    assert len(in_flight) == 1
    assert in_flight[0].startswith("date=2026-08-24.parquet.tmp-")
    assert bucket.rsync_excluded(f"chains/ticker=SPY/{in_flight[0]}", is_dir=False)
    assert (root / SPY_1).read_bytes() == originals[SPY_1]
    assert _strays(root) == []


@pytest.mark.parametrize("stop", [KeyboardInterrupt(), RuntimeError("a client bug")])
def test_an_interrupt_or_a_bug_mid_download_leaves_no_temp_in_the_lake(tmp_path, monkeypatch, stop):
    """Mutation this catches: removing the temp only on the refusals the command names.

    A leftover is an orphan to the Sunday scrub, which withholds its ping, and only a re-run
    over the same target would remove it.
    """
    root, client, _originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    seen: list[str] = []
    _watch_downloads(monkeypatch, root, seen, stop=stop)

    with pytest.raises(type(stop)):
        _run(root, client, last=D1)

    assert any(".tmp-" in name for name in seen)
    assert _strays(root) == []
    assert not (root / SPY_1).exists()


def test_a_leftover_temp_from_a_dead_process_is_removed_by_the_next_run(tmp_path):
    root, client, _originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1)
    leftover = root / "chains/ticker=SPY/date=2026-08-24.parquet.tmp-424242"
    leftover.write_bytes(b"what a killed run left")

    summary = _run(root, client, last=D1)

    assert summary.temps_removed == ["chains/ticker=SPY/date=2026-08-24.parquet.tmp-424242"]
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


def test_the_reserve_is_measured_on_the_lake_as_it_stands_after_the_restore(tmp_path):
    """A restored day inside the growth window can become the busiest one.

    Both of the day's chains partitions are lost, so on disk the day holds only its quotes
    partition and reads smaller than the days around it. Restored, it is the busiest day, and
    the Lake panel reads the reserve off it from then on. Mutation this catches: measuring the
    busiest day before the restore, which passes at the old boundary.
    """
    from lake import runway

    root, client, originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    (root / QQQ_1).unlink()
    planned = len(originals[SPY_1]) + len(originals[QQQ_1])
    usage = runway.walk(root)
    before = runway.busiest_sealed_day(usage, today=MONDAY_19.date())
    after = usage.sealed_bytes(D1) + planned
    assert after > before
    # 13 is the reserve's session count, written as a literal rather than read from the code.
    old_boundary = planned + 13 * before
    new_boundary = planned + 13 * after

    assert "short of the journal reserve" in _refuses(
        root, client, ticker=None, last=D1, free=old_boundary
    )
    assert "short of the journal reserve" in _refuses(
        root, client, ticker=None, last=D1, free=new_boundary - 1
    )
    assert not (root / SPY_1).exists() and not (root / QQQ_1).exists()
    assert _run(root, client, ticker=None, last=D1, free=new_boundary).restored == 2


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
    assert "Run it on the primary" in err[0]
    assert "uploads nothing" not in err[0]
    assert client.calls == []
    assert not (root / SPY_1).exists()


def _argv(config: Path, first: str = "2026-08-24", last: str = "2026-08-26") -> list[str]:
    return [
        "restore-range",
        "--surface",
        "chains",
        "--ticker",
        "SPY",
        "--from",
        first,
        "--to",
        last,
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


# -- the lock is never held across a hash ------------------------------------------


def test_no_present_partition_is_hashed_under_the_lock(tmp_path, monkeypatch):
    """Capture's close+5 fill waits on the lock with no timeout, and a rollback's present files
    run to gigabytes. Mutation this catches: hashing a present file inside the lock hold.
    """
    root, client, _originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1, unlink=False)
    (root / SPY_2).unlink()
    held: list[bool] = []
    hashed: list[tuple[str, bool]] = []
    real_lock = bucket.lake_lock
    real_sha = bucket.sha256_file

    @contextmanager
    def watched_lock(lake_root):
        with real_lock(lake_root) as path:
            held.append(True)
            try:
                yield path
            finally:
                held.pop()

    def watched_sha(path):
        hashed.append((Path(path).relative_to(root).as_posix(), bool(held)))
        return real_sha(path)

    monkeypatch.setattr(bucket, "lake_lock", watched_lock)
    monkeypatch.setattr(bucket, "sha256_file", watched_sha)

    summary = _run(root, client)

    assert sorted(rel for rel, _locked in hashed) == [SPY_1, SPY_3]
    assert [rel for rel, locked in hashed if locked] == []
    assert (summary.restored, summary.restore_lines) == (1, 1)
    assert _kind(root, SPY_1) == "restore"


# -- every refusal handler reaches the operator as one line ----------------------


def _main_refusal(tmp_path, root, client, monkeypatch, capsys, **argv) -> str:
    config = write_config(tmp_path, root)
    config.write_text(config.read_text() + KEYS)
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)

    with pytest.raises(SystemExit) as exc:
        bucket.main(_argv(config, **argv), clock=ManualClock(MONDAY_19), calendar=CALENDAR)

    assert exc.value.code == 2
    captured = capsys.readouterr()
    lines = captured.err.splitlines()
    assert len(lines) == 1, captured.err
    assert lines[0].startswith("restore-range: ")
    assert "Traceback" not in captured.err
    return lines[0]


def test_a_reversed_range_refuses(tmp_path, monkeypatch, capsys):
    root, client, _originals = _lake(tmp_path)

    line = _main_refusal(
        tmp_path, root, client, monkeypatch, capsys, first="2026-08-26", last="2026-08-24"
    )

    assert "before it starts" in line


def test_a_damaged_manifest_refuses(tmp_path, monkeypatch, capsys):
    root, client, _originals = _lake(tmp_path)
    manifest = root / "manifest.jsonl"
    manifest.write_bytes(manifest.read_bytes() + b'{"partition": "\xff"}\n')

    line = _main_refusal(tmp_path, root, client, monkeypatch, capsys)

    assert "manifest.jsonl" in line and "nothing was restored" in line
    assert "records no" not in line


def test_a_ledger_the_repair_refuses_stops_the_run(tmp_path, monkeypatch, capsys):
    root, client, _originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1)
    ledger = trimmed_path(root)
    ledger.write_bytes(ledger.read_bytes().replace(b'"version_id": "v', b'"version_id": "w'))

    line = _main_refusal(tmp_path, root, client, monkeypatch, capsys)

    assert "edited in place" in line and "Nothing was restored" in line
    assert not (root / SPY_1).exists()


def test_a_restore_line_the_ledger_refuses_in_the_repair_refuses(tmp_path, monkeypatch, capsys):
    root, client, _originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1, unlink=False)

    def lost(*args, **kwargs):
        raise TrimmedLineLost("trimmed.jsonl: the line was lost")

    monkeypatch.setattr(trimmed, "append_trimmed", lost)

    line = _main_refusal(tmp_path, root, client, monkeypatch, capsys)

    assert "the line was lost" in line


def test_a_ticker_directory_that_will_not_list_refuses(tmp_path, monkeypatch, capsys):
    root, client, _originals = _lake(tmp_path)
    locked = root / "chains/ticker=SPY"
    locked.chmod(0)
    try:
        line = _main_refusal(tmp_path, root, client, monkeypatch, capsys)
    finally:
        locked.chmod(0o755)

    assert "PermissionError" in line and "reading or repairing" in line


def test_free_space_that_cannot_be_read_refuses(tmp_path, monkeypatch, capsys):
    root, client, _originals = _lake(tmp_path)
    (root / SPY_1).unlink()

    def broken(path):
        raise OSError(errno.EIO, "Input/output error")

    monkeypatch.setattr(bucket.shutil, "disk_usage", broken)

    line = _main_refusal(tmp_path, root, client, monkeypatch, capsys)

    assert "reading free space" in line


def test_a_temp_that_cannot_be_written_refuses(tmp_path, monkeypatch, capsys):
    root, client, _originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    directory = root / "chains/ticker=SPY"
    directory.chmod(0o555)
    try:
        line = _main_refusal(tmp_path, root, client, monkeypatch, capsys)
    finally:
        directory.chmod(0o755)

    assert "writing chains/ticker=SPY/date=2026-08-24.parquet.tmp-" in line
    assert _strays(root) == []


def test_a_flush_that_fails_at_the_commit_refuses(tmp_path, monkeypatch, capsys):
    root, client, _originals = _lake(tmp_path)
    (root / SPY_1).unlink()

    def failing(path):
        raise OSError(errno.EIO, "Input/output error")

    monkeypatch.setattr(bucket, "_fsync_path", failing)

    line = _main_refusal(tmp_path, root, client, monkeypatch, capsys)

    assert "committing" in line and SPY_1 in line
    assert _strays(root) == []


def test_a_partition_gone_from_the_bucket_mid_run_refuses(tmp_path, monkeypatch, capsys):
    root, client, _originals = _lake(tmp_path)
    version = _trim_away(root, client, SPY_1)

    def absent(**kwargs):
        raise client_error("NoSuchKey", "GetObject", 404)

    monkeypatch.setattr(client, "get_object", absent)

    line = _main_refusal(tmp_path, root, client, monkeypatch, capsys)

    assert "holds no current version" in line and f"version {version}" in line
    assert _strays(root) == []


def test_a_ledger_that_cannot_be_read_at_the_commit_refuses(tmp_path, monkeypatch, capsys):
    root, client, _originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    real = trimmed.latest_trimmed
    calls: list[int] = []

    def flaky(lake_root):
        calls.append(1)
        if len(calls) > 1:
            raise ManifestError("trimmed.jsonl: unreadable at the commit")
        return real(lake_root)

    monkeypatch.setattr(trimmed, "latest_trimmed", flaky)

    line = _main_refusal(tmp_path, root, client, monkeypatch, capsys, last="2026-08-24")

    assert "unreadable at the commit" in line and "stopped at" in line
    assert not (root / SPY_1).exists()
    assert _strays(root) == []


def test_a_present_partition_that_cannot_be_read_refuses(tmp_path, monkeypatch, capsys):
    root, client, _originals = _lake(tmp_path)
    unreadable = root / SPY_3
    unreadable.chmod(0)
    try:
        line = _main_refusal(tmp_path, root, client, monkeypatch, capsys)
    finally:
        unreadable.chmod(0o644)

    assert "hashing the chains partitions" in line and "PermissionError" in line


def test_a_restore_line_the_disk_refuses_in_the_repair_refuses(tmp_path, monkeypatch, capsys):
    root, client, _originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1, unlink=False)

    def full(*args, **kwargs):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(trimmed, "append_trimmed", full)

    line = _main_refusal(tmp_path, root, client, monkeypatch, capsys)

    assert "writing a restore line" in line and "OSError" in line


def _run_reaching_the_bound_at_lock(root, client, monkeypatch, number: int) -> None:
    """Run from 08:30, with the clock reaching the 09:00 bound as lock ``number`` is taken.

    A lock can be waited on past the bound, behind compaction or a capture cycle, so each
    hold checks the bound again once it has the lock.
    """
    clock = ManualClock(datetime(2026, 8, 31, 8, 30, tzinfo=MARKET_TZ))
    real_lock = bucket.lake_lock
    taken: list[int] = []

    @contextmanager
    def slow_lock(lake_root):
        with real_lock(lake_root) as path:
            taken.append(1)
            if len(taken) == number:
                clock.set(MONDAY_BOUND)
            yield path

    monkeypatch.setattr(bucket, "lake_lock", slow_lock)
    with pytest.raises(RangeRestoreRefused, match="next session"):
        restore_range(
            root,
            TARGET,
            client=client,
            clock=clock,
            calendar=CALENDAR,
            surface="chains",
            ticker="SPY",
            first=D1,
            last=D1,
            free_space=lambda path: PLENTY,
        )
    assert len(taken) == number


def test_the_bound_is_checked_again_once_the_first_lock_is_taken(tmp_path, monkeypatch):
    """Mutation this catches: no check after the first lock, which runs the ledger repair."""
    root, client, _originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1)
    with trimmed_path(root).open("ab") as handle:
        handle.write(b'{"kind": "restore", "partition": "x", "restored_at": "t", "sha256": "s"}\n')
    entry = latest_entries(root)["trimmed.jsonl"]

    _run_reaching_the_bound_at_lock(root, client, monkeypatch, 1)

    assert latest_entries(root)["trimmed.jsonl"] == entry
    assert not (root / SPY_1).exists()


def test_the_bound_is_checked_again_before_the_owed_restore_lines(tmp_path, monkeypatch):
    """Mutation this catches: no check after the lock the owed restore lines are written in."""
    root, client, _originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1, unlink=False)

    _run_reaching_the_bound_at_lock(root, client, monkeypatch, 2)

    assert _kind(root, SPY_1) == "trim"


def test_the_bound_is_checked_again_before_a_commit(tmp_path, monkeypatch):
    """Mutation this catches: no check after a partition's commit lock, so a download that ran
    into the pre-open margin still commits there.
    """
    root, client, _originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1)

    _run_reaching_the_bound_at_lock(root, client, monkeypatch, 2)

    assert not (root / SPY_1).exists()
    assert _kind(root, SPY_1) == "trim"
    assert _strays(root) == []


def _recompact(root: Path, rel: str) -> None:
    """Reseal a partition with new bytes and a new entry, as a recompaction by hand does."""
    with lake_lock(root):
        data = (root / rel).read_bytes() + b"recompacted"
        (root / rel).write_bytes(data)
        append_manifest(
            root,
            partition=rel,
            source="recompact",
            sha256=hashlib.sha256(data).hexdigest(),
            rows=latest_entries(root)[rel]["rows"],
            fetched_at=STAMP,
            guard=False,
        )


def test_a_file_gone_after_the_hash_is_restored_rather_than_counted_present(tmp_path, monkeypatch):
    """The trim that crashed before its unlink finishes it between the hash and the lock.

    Mutations this catches: writing the owed line without the re-check, which supersedes the
    trim line of a file that is gone, and counting the file present, which reports success
    over a selected partition that is absent.
    """
    root, client, originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1, unlink=False)
    real_sha = bucket.sha256_file

    def hash_then_trim(path):
        digest = real_sha(path)
        if Path(path) == root / SPY_1:
            Path(path).unlink()
        return digest

    monkeypatch.setattr(bucket, "sha256_file", hash_then_trim)

    summary = _run(root, client, last=D1)

    assert (summary.present, summary.restored, summary.restore_lines) == (0, 1, 1)
    assert (root / SPY_1).read_bytes() == originals[SPY_1]
    assert _kind(root, SPY_1) == "restore"
    assert scrub(root).ok


def test_a_file_gone_before_the_hash_is_restored_rather_than_refused(tmp_path, monkeypatch):
    """Mutation this catches: reading the vanished file as a local failure to fix by hand."""
    root, client, originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1, unlink=False)
    real_sha = bucket.sha256_file

    def trim_then_hash(path):
        if Path(path) == root / SPY_1:
            Path(path).unlink()
        return real_sha(path)

    monkeypatch.setattr(bucket, "sha256_file", trim_then_hash)

    summary = _run(root, client, last=D1)

    assert (summary.present, summary.restored, summary.restore_lines) == (0, 1, 1)
    assert (root / SPY_1).read_bytes() == originals[SPY_1]
    assert scrub(root).ok


def test_a_recompaction_between_the_plan_and_the_hash_says_run_again(tmp_path, monkeypatch):
    """A recompaction by hand takes the lock and has no session bound, so it can land between
    the planning lock and the unlocked hash. The lake it leaves is consistent.

    Mutation this catches: telling the operator to move a good file out of the lake.
    """
    root, client, _originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    real_sha = bucket.sha256_file
    done: list[int] = []

    def recompact_then_hash(path):
        if Path(path) == root / SPY_3 and not done:
            done.append(1)
            _recompact(root, SPY_3)
        return real_sha(path)

    monkeypatch.setattr(bucket, "sha256_file", recompact_then_hash)

    line = _refuses_after_commit(root, client)

    assert f"{SPY_3} changed in the lake while the range restore ran" in line
    assert "Move it out of the lake" not in line
    assert SPY_3 not in scrub(root).sha_mismatches
    assert not (root / SPY_1).exists()


def test_a_recompaction_after_the_hash_writes_no_stale_restore_line(tmp_path, monkeypatch):
    """Mutation this catches: dropping the entry's sha from the owed line's re-check, which
    writes a restore line carrying a sha the manifest no longer records.
    """
    root, client, _originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1, unlink=False)
    real_sha = bucket.sha256_file
    done: list[int] = []

    def hash_then_recompact(path):
        digest = real_sha(path)
        if Path(path) == root / SPY_1 and not done:
            done.append(1)
            _recompact(root, SPY_1)
        return digest

    monkeypatch.setattr(bucket, "sha256_file", hash_then_recompact)

    line = _refuses_after_commit(root, client)

    assert "changed in the lake while the range restore ran" in line
    assert _kind(root, SPY_1) == "trim"


def test_a_file_that_still_differs_under_the_lock_is_refused_as_rot(tmp_path, monkeypatch):
    """The re-check reads the entry again. An entry that did not move means the bytes did."""
    root, client, _originals = _lake(tmp_path)
    (root / SPY_3).write_bytes(b"rotted bytes")

    line = _refuses(root, client)

    assert "does not match its manifest entry" in line
    assert "changed in the lake" not in line


def test_an_owed_restore_line_is_not_written_twice(tmp_path, monkeypatch):
    """Another run wrote the line between this run's hash and its lock.

    A second identical line breaks no absence rule, and it is still a line the ledger did not
    need. Mutation this catches: dropping the re-check that the latest line is still a trim
    line.
    """
    root, client, _originals = _lake(tmp_path)
    sha = latest_entries(root)[SPY_1]["sha256"]
    _trim_away(root, client, SPY_1, unlink=False)
    real_sha = bucket.sha256_file

    def hash_then_restore(path):
        digest = real_sha(path)
        if Path(path) == root / SPY_1:
            with lake_lock(root):
                append_trimmed(
                    root,
                    restore_line(SPY_1, sha256=sha, restored_at=STAMP),
                    source="another-run",
                    fetched_at=STAMP,
                )
        return digest

    monkeypatch.setattr(bucket, "sha256_file", hash_then_restore)

    summary = _run(root, client, last=D1)

    assert summary.restore_lines == 0
    kinds = [line["kind"] for line in read_trimmed(root) if line["partition"] == SPY_1]
    assert kinds == ["trim", "restore"]


def test_a_temp_the_directory_will_not_unlink_keeps_the_one_line(tmp_path, monkeypatch, capsys):
    """A read-only remount after the temp landed: the cleanup's own unlink is refused too.

    Mutation this catches: the cleanup catching only a missing file, so its ``PermissionError``
    replaces the one-line refusal with a traceback.
    """
    root, client, _originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    parent = (root / SPY_1).parent

    def landed_then_refused(read, rel, temp):
        Path(temp).write_bytes(b"partial")
        parent.chmod(0o555)
        raise OSError(errno.EROFS, "Read-only file system")

    monkeypatch.setattr(bucket, "_download_to", landed_then_refused)
    try:
        line = _main_refusal(tmp_path, root, client, monkeypatch, capsys, last="2026-08-24")
    finally:
        parent.chmod(0o755)

    assert "Read-only file system" in line and "writing" in line


def test_a_bucket_refusal_mid_run_says_how_far_it_got(tmp_path, monkeypatch, capsys):
    """Mutation this catches: the download's bucket failure escaping as itself, so ``main``
    prints its generic line and the count of partitions already restored is lost.
    """
    root, client, originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    (root / SPY_2).unlink()
    real_get = client.get_object

    def second_denied(**kwargs):
        if kwargs.get("Key") == TARGET.key(SPY_2):
            raise client_error("AccessDenied", "GetObject", 403)
        return real_get(**kwargs)

    monkeypatch.setattr(client, "get_object", second_denied)

    line = _main_refusal(tmp_path, root, client, monkeypatch, capsys)

    assert "AccessDenied" in line
    assert "1 partition(s) were restored before this stop" in line
    assert (root / SPY_1).read_bytes() == originals[SPY_1]
    assert _strays(root) == []


# -- ported from the mutation lens on PR #810 ---------------------------------------


def test_a_session_reached_during_the_run_stops_before_the_download(tmp_path, monkeypatch):
    root, client, _originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    clock = ManualClock(datetime(2026, 8, 31, 8, 0, tzinfo=MARKET_TZ))
    listing = bucket.list_bucket

    def slow_listing(*args, **kwargs):
        result = listing(*args, **kwargs)
        clock.set(MONDAY_BOUND)
        return result

    monkeypatch.setattr(bucket, "list_bucket", slow_listing)

    with pytest.raises(RangeRestoreRefused, match="next session"):
        restore_range(
            root,
            TARGET,
            client=client,
            clock=clock,
            calendar=CALENDAR,
            surface="chains",
            ticker="SPY",
            first=D1,
            last=D1,
            free_space=lambda path: PLENTY,
        )

    assert not [kwargs for name, kwargs in client.calls if name == "get_object"]
    assert not (root / SPY_1).exists()


def test_the_restored_file_and_its_directory_are_flushed(tmp_path, monkeypatch):
    root, client, _originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    flushed: list[int] = []
    fsync = os.fsync

    def spy(fd):
        flushed.append(os.fstat(fd).st_ino)
        return fsync(fd)

    monkeypatch.setattr(bucket.os, "fsync", spy)

    _run(root, client, last=D1)

    assert (root / SPY_1).stat().st_ino in flushed
    assert (root / SPY_1).parent.stat().st_ino in flushed


def test_a_rename_failure_refuses_keeps_no_temp_and_counts_nothing(tmp_path, monkeypatch):
    root, client, _originals = _lake(tmp_path)
    (root / SPY_1).unlink()

    def denied(source, destination):
        raise PermissionError(errno.EACCES, "Permission denied")

    monkeypatch.setattr(bucket.os, "replace", denied)

    line = _refuses(root, client, last=D1)

    assert "Permission denied" in line
    assert "restored before this stop" not in line
    assert _strays(root) == []
    assert not (root / SPY_1).exists()


def test_the_summary_counts_the_bytes_and_names_the_bucket(tmp_path):
    root, client, originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    (root / SPY_2).unlink()

    summary = _run(root, client)

    assert summary.restored_bytes == len(originals[SPY_1]) + len(originals[SPY_2])
    assert summary.target == str(TARGET)


def test_a_run_with_nothing_to_download_never_reaches_the_bucket(tmp_path):
    root, client, _originals = _lake(tmp_path)
    client.calls.clear()

    summary = _run(root, client, free=0)

    assert summary.present == 3 and summary.restored == 0
    assert client.calls == []


def test_a_ticker_whose_directory_is_gone_comes_back(tmp_path):
    root, client, originals = _lake(tmp_path)
    (root / QQQ_1).unlink()
    (root / QQQ_1).parent.rmdir()

    summary = _run(root, client, ticker="QQQ", last=D1)

    assert summary.restored == 1
    assert (root / QQQ_1).read_bytes() == originals[QQQ_1]


def test_the_repaired_restore_line_records_the_manifest_sha(tmp_path):
    root, client, originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1, unlink=False)

    _run(root, client, last=D1)

    line = latest_trimmed(root)[SPY_1]
    assert line["kind"] == "restore"
    assert line["sha256"] == hashlib.sha256(originals[SPY_1]).hexdigest()
    assert line["restored_at"] == MONDAY_19.isoformat()


def test_the_ledger_repair_names_the_range_restore_as_its_writer(tmp_path, monkeypatch):
    root, client, _originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1)

    def crash(*args, **kwargs):
        raise Crash("killed between the append and the record")

    monkeypatch.setattr(trimmed, "_record", crash)
    with pytest.raises(Crash):
        _run(root, client, last=D1)
    monkeypatch.undo()

    assert _run(root, client, last=D1).ledger_repaired
    entry = latest_entries(root)["trimmed.jsonl"]
    assert entry["source"] == bucket.RANGE_RESTORE_SOURCE
    assert entry["fetched_at"] == MONDAY_19.isoformat()


def test_the_summary_line_names_every_count(tmp_path):
    summary = bucket.RangeRestoreSummary(
        target="s3://b/p",
        selected=5,
        restored=2,
        restored_bytes=3_500_000,
        present=3,
        restore_lines=1,
        ledger_repaired=True,
        temps_removed=["a.tmp-1"],
    )

    assert summary.render() == (
        "restored 2 of 5 selected partition(s) from s3://b/p (3.5 MB), 3 already present, "
        "1 restore line(s) written, 1 leftover temp file(s) removed, and the trimmed "
        "ledger's manifest entry re-recorded"
    )


def test_the_command_prints_each_removed_temp(tmp_path, monkeypatch, capsys):
    root, client, _originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    (root / (SPY_1 + ".tmp-99999")).write_bytes(b"half")
    config = write_config(tmp_path, root)
    config.write_text(config.read_text() + KEYS)
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)

    code = bucket.main(_argv(config), clock=ManualClock(MONDAY_19), calendar=CALENDAR)

    assert code == 0
    out = capsys.readouterr().out.splitlines()
    assert out[0] == f"restore-range: removed a leftover temp file: {SPY_1}.tmp-99999"
    assert out[1].startswith("restore-range: restored 1 of 3")


@pytest.mark.parametrize("bad", ["20260824", "2026-8-24", "2026-08-32"])
def test_a_date_argument_not_written_as_yyyy_mm_dd_exits_2(tmp_path, capsys, bad):
    config = write_config(tmp_path, tmp_path / "lake")
    argv = _argv(config)
    argv[argv.index("--from") + 1] = bad

    with pytest.raises(SystemExit) as exc:
        bucket.main(argv, clock=ManualClock(MONDAY_19), calendar=CALENDAR)

    assert exc.value.code == 2
    assert "expected a date as YYYY-MM-DD" in capsys.readouterr().err


def test_leftover_temps_are_removed_and_reported_in_name_order(tmp_path):
    root, client, _originals = _lake(tmp_path)
    (root / SPY_1).unlink()
    names = [f"{SPY_1}.tmp-{pid}" for pid in (9, 7, 12, 31, 100, 4, 2, 5, 88, 61)]
    for name in names:
        (root / name).write_bytes(b"half")

    summary = _run(root, client, last=D1)

    assert summary.temps_removed == sorted(names)


def test_the_ledger_repair_runs_even_when_the_selection_refuses(tmp_path, monkeypatch):
    root, client, _originals = _lake(tmp_path)
    _trim_away(root, client, SPY_1)

    def crash(*args, **kwargs):
        raise Crash("killed between the append and the record")

    monkeypatch.setattr(trimmed, "_record", crash)
    with pytest.raises(Crash):
        _run(root, client, last=D1)
    monkeypatch.undo()

    with pytest.raises(RangeRestoreRefused, match="records no chains partitions"):
        _run(root, client, ticker="SPYY")

    assert latest_entries(root)["trimmed.jsonl"]["sha256"] == sha256_file(trimmed_path(root))
