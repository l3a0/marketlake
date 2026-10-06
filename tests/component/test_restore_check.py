"""The weekly restore test, across one real boundary: the filesystem.

``manifest.restore_check`` reads the week's files back out of a backup and hashes the
bytes as they arrive. These tests put literal files under a temporary target and read
them through ``path_reader`` or through ``FakeBackupReader``, which serves the real bytes
unless told to lie about a path.

The paths are chosen by residue, computed once by hand from ``hashlib``. The three
``R3_`` files share residue 3, so week 3 reads all three in path order, and ``R7`` sits
alone in residue 7.

The backup scrub's half of the hand-over, ``matched`` and ``walked``, is tested here
too, since those two fields exist for this reader and nothing else reads them.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import pytest

from lake import manifest
from lake.manifest import (
    BackupScrubResult,
    RestoreResult,
    backup_scrub,
    path_reader,
    restore_check,
)
from lake.paths import MANIFEST_FILE
from tests.support.backup import FAIL, FAIL_MIDWAY, WRONG, FakeBackupReader, mirror_lake
from tests.support.lake import FixtureLake, sample_chains_table

R3_A = "chains/ticker=SPY/date=2026-05-15.parquet"  # residue 3
R3_B = "chains/ticker=SPY/date=2026-08-04.parquet"  # residue 3
R3_C = "chains/ticker=SPY/date=2026-09-13.parquet"  # residue 3
R7 = "chains/ticker=SPY/date=2026-02-05.parquet"  # residue 7

CONTENT = {R3_A: b"alpha", R3_B: b"bravo!", R3_C: b"charlie", R7: b"delta"}


def _target(tmp_path: Path) -> tuple[Path, list[tuple[str, str]]]:
    """Four files on a target, and the pairs a backup scrub would have matched them to."""
    target = tmp_path / "ssd"
    for rel, data in CONTENT.items():
        path = target / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    pairs = [(rel, hashlib.sha256(data).hexdigest()) for rel, data in CONTENT.items()]
    return target, pairs


def _tree(root: Path) -> list[tuple[str, int, int]]:
    """Every file under a root with its size and modification time."""
    return sorted(
        (p.relative_to(root).as_posix(), p.stat().st_size, p.stat().st_mtime_ns)
        for p in root.rglob("*")
    )


# -- a pass --------------------------------------------------------------------------


def test_a_week_whose_files_all_match_passes_and_says_what_it_read(tmp_path):
    target, pairs = _target(tmp_path)
    reader = FakeBackupReader(target)

    result = restore_check(target, pairs, 3, reader)

    assert reader.calls == [R3_A, R3_B, R3_C]
    assert result.restored == (R3_A, R3_B, R3_C)
    assert result.bytes_read == 18  # 5 + 6 + 7
    assert result.mismatches == () and result.unreadable is None
    assert result.problem is None and result.ok is True
    assert result.notes == ()
    assert result.pass_line == (
        f"3 files (0.0 MB) read back from {target} matched the manifest, "
        "week of Sunday 2026-01-25, rotation slot 3 of 52"
    )


def test_the_real_path_reader_passes_the_same_week(tmp_path):
    target, pairs = _target(tmp_path)
    result = restore_check(target, pairs, 3, path_reader(target))
    assert result.restored == (R3_A, R3_B, R3_C)
    assert result.ok is True


def test_a_file_read_in_many_chunks_is_hashed_whole(tmp_path, monkeypatch):
    # Two-byte chunks split every file, so a hash of the first chunk alone matches none.
    monkeypatch.setattr(manifest, "_RESTORE_CHUNK", 2)
    target, pairs = _target(tmp_path)
    result = restore_check(target, pairs, 3, path_reader(target))
    assert result.restored == (R3_A, R3_B, R3_C)
    assert result.ok is True
    assert result.bytes_read == 18


def test_rot_past_the_first_chunk_is_a_mismatch(tmp_path):
    # The first byte arrives sound and the second is wrong, so only a hash over every
    # chunk can see it.
    target, pairs = _target(tmp_path)

    def reader(rel):
        data = (target / rel).read_bytes()
        return iter([data[:1], b"X" + data[2:]])

    result = restore_check(target, pairs, 3, reader)
    assert result.mismatches == (R3_A, R3_B, R3_C)
    assert result.restored == ()


def test_a_week_that_fell_through_names_the_slot_it_read(tmp_path):
    target, pairs = _target(tmp_path)
    # Slots 4 to 6 hold nothing, so week 4 reads slot 7.
    result = restore_check(target, pairs, 4, FakeBackupReader(target))
    assert result.restored == (R7,)
    assert result.residue == 7
    assert result.pass_line == (
        f"1 file (0.0 MB) read back from {target} matched the manifest, "
        "week of Sunday 2026-02-01, rotation slot 4 of 52, from slot 7 because slot 4 held "
        "no files"
    )


def test_a_week_past_the_first_cycle_that_did_not_fall_through_says_nothing_more(tmp_path):
    target, pairs = _target(tmp_path)
    result = restore_check(target, pairs, 55, FakeBackupReader(target))  # 55 is slot 3
    assert result.residue == 3
    assert result.pass_line == (
        f"3 files (0.0 MB) read back from {target} matched the manifest, "
        "week of Sunday 2027-01-24, rotation slot 3 of 52"
    )


def test_the_pass_line_gives_megabytes_to_one_decimal():
    # A megabyte is a million bytes here, and the figure rounds rather than truncates.
    def line(size):
        result = RestoreResult(
            target="/ssd", week=39, candidates=1, residue=39, restored=("a",), bytes_read=size
        )
        return result.pass_line

    assert line(12_400_000) == (
        "1 file (12.4 MB) read back from /ssd matched the manifest, "
        "week of Sunday 2026-10-04, rotation slot 39 of 52"
    )
    assert "(12.5 MB)" in line(12_460_000)


def test_nothing_matched_is_said_plainly_and_withholds_nothing(tmp_path):
    reader = FakeBackupReader(tmp_path)
    result = restore_check(tmp_path, [], 39, reader)
    assert reader.calls == []
    assert result.problem is None and result.ok is True
    assert result.pass_line == (
        "nothing to restore, the backup scrub matched no files, "
        "week of Sunday 2026-10-04, rotation slot 39 of 52"
    )
    assert result.notes == ()


def test_the_restore_writes_nothing(tmp_path):
    target, pairs = _target(tmp_path)
    before = _tree(tmp_path)
    restore_check(target, pairs, 3, path_reader(target))
    assert _tree(tmp_path) == before


# -- findings ------------------------------------------------------------------------


def test_a_file_that_reads_back_wrong_is_named_and_the_rest_are_still_read(tmp_path):
    target, pairs = _target(tmp_path)
    reader = FakeBackupReader(target, faults={R3_A: WRONG})

    result = restore_check(target, pairs, 3, reader)

    assert reader.calls == [R3_A, R3_B, R3_C]
    assert result.mismatches == (R3_A,)
    assert result.restored == (R3_B, R3_C)
    assert result.ok is False
    assert result.problem == f"restore test failed: mismatches=1 unreadable=0: {target}"
    assert result.pass_line is None
    # A path mismatch is two reads disagreeing, so the line sends the operator to the disk
    # and names the one benign cause rather than asking for a re-copy.
    assert result.notes == (
        f"restore read back bytes that do not match the manifest: {R3_A}",
        "restore mismatch: the backup scrub matched these files moments earlier, so two "
        "reads of one file returned different bytes. Check the disk and its cable, then "
        "re-run the Sunday job. A hand-run lake.compact that replaced the file between the "
        "two reads causes the same mismatch, and a re-run clears it",
    )


def test_two_mismatches_and_a_failed_read_are_all_named_in_order(tmp_path):
    target, pairs = _target(tmp_path)
    reader = FakeBackupReader(target, faults={R3_A: WRONG, R3_B: WRONG, R3_C: FAIL})

    result = restore_check(target, pairs, 3, reader)

    assert result.problem == f"restore test failed: mismatches=2 unreadable=1: {target}"
    assert result.pass_line is None
    label = "restore read back bytes that do not match the manifest"
    notes = result.notes
    # Each mismatch is named, then one repair line covers them all, then the failed read.
    assert notes[:2] == (f"{label}: {R3_A}", f"{label}: {R3_B}")
    assert notes[2].startswith("restore mismatch:")
    assert notes[3] == f"restore could not read: {R3_C}: OSError: fake read failed: {R3_C}"
    assert notes[4].startswith("restore stopped at that read")
    assert len(notes) == 5


def test_mismatched_files_are_named_up_to_the_cap():
    result = RestoreResult(target="/ssd", week=3, candidates=4, mismatches=("a", "b", "c", "d"))
    label = "restore read back bytes that do not match the manifest"
    assert result.notes[:4] == (
        f"{label}: a",
        f"{label}: b",
        f"{label}: c",
        f"{label}: and 1 more",
    )
    assert result.problem == "restore test failed: mismatches=4 unreadable=0: /ssd"


def test_a_failed_read_stops_the_restore_and_still_names_the_mismatch_before_it(tmp_path):
    target, pairs = _target(tmp_path)
    reader = FakeBackupReader(target, faults={R3_A: WRONG, R3_B: FAIL})

    result = restore_check(target, pairs, 3, reader)

    # The third file is never asked for.
    assert reader.calls == [R3_A, R3_B]
    assert result.mismatches == (R3_A,)
    assert result.restored == ()
    assert result.unreadable == f"{R3_B}: OSError: fake read failed: {R3_B}"
    assert result.ok is False
    assert result.problem == f"restore test failed: mismatches=1 unreadable=1: {target}"
    assert result.pass_line is None
    assert f"restore could not read: {R3_B}: OSError: fake read failed: {R3_B}" in result.notes
    assert f"restore read back bytes that do not match the manifest: {R3_A}" in result.notes
    assert any("files after it were not checked" in line for line in result.notes)


def test_a_failed_read_alone_prints_no_pass_line(tmp_path):
    target, pairs = _target(tmp_path)
    result = restore_check(target, pairs, 3, FakeBackupReader(target, faults={R3_A: FAIL}))
    assert result.mismatches == ()
    assert result.problem == f"restore test failed: mismatches=0 unreadable=1: {target}"
    assert result.pass_line is None


def test_a_read_that_fails_partway_through_its_bytes_is_caught_too(tmp_path):
    target, pairs = _target(tmp_path)
    reader = FakeBackupReader(target, faults={R3_A: FAIL_MIDWAY})

    result = restore_check(target, pairs, 3, reader)

    assert reader.calls == [R3_A]
    assert result.unreadable == f"{R3_A}: OSError: fake read failed partway: {R3_A}"
    assert result.mismatches == ()
    assert result.problem == f"restore test failed: mismatches=0 unreadable=1: {target}"
    # The one chunk that arrived before the failure is counted.
    assert result.bytes_read == 1


def test_bytes_read_counts_wrong_bytes_and_candidates_counts_every_pair(tmp_path):
    # The fake's wrong bytes are 30 long, and the two sound files after it are 6 and 7.
    target, pairs = _target(tmp_path)
    result = restore_check(target, pairs, 3, FakeBackupReader(target, faults={R3_A: WRONG}))
    assert result.bytes_read == 30 + 6 + 7
    # Four pairs were handed over and three were read.
    assert result.candidates == 4


def test_a_file_gone_from_a_path_target_is_a_failed_read_not_a_raise(tmp_path):
    target, pairs = _target(tmp_path)
    (target / R3_A).unlink()
    result = restore_check(target, pairs, 3, path_reader(target))
    assert result.unreadable is not None
    assert result.unreadable.startswith(f"{R3_A}: FileNotFoundError: ")
    assert result.restored == ()


def test_an_error_that_is_not_a_read_failure_still_raises(tmp_path):
    # The catch is for ``OSError`` alone, so a bug in a reader is a traceback rather than
    # a finding that names the disk.
    target, pairs = _target(tmp_path)

    def broken(rel):
        raise ValueError("a bug, not a disk")

    with pytest.raises(ValueError):
        restore_check(target, pairs, 3, broken)


# -- the path reader -----------------------------------------------------------------


def test_the_path_reader_reads_a_large_file_in_chunks(tmp_path, monkeypatch):
    monkeypatch.setattr(manifest, "_RESTORE_CHUNK", 4)
    (tmp_path / "f.bin").write_bytes(b"0123456789")
    chunks = list(path_reader(tmp_path)("f.bin"))
    assert chunks == [b"0123", b"4567", b"89"]


# -- the backup scrub's hand-over ----------------------------------------------------

DAY = date(2026, 8, 28)
CHAINS = f"chains/ticker=SPY/date={DAY.isoformat()}.parquet"
QUOTES = f"quotes/ticker=SPY/date={DAY.isoformat()}.parquet"


def _shas(root: Path) -> dict[str, list[str]]:
    """Every sha the lake's manifest recorded for each path, oldest first."""
    shas: dict[str, list[str]] = {}
    for line in (root / MANIFEST_FILE).read_text().splitlines():
        entry = json.loads(line)
        shas.setdefault(entry["partition"], []).append(entry["sha256"])
    return shas


def _backed_up(fixture_lake: FixtureLake) -> tuple[Path, Path]:
    fixture_lake.with_chains("SPY", DAY)
    fixture_lake.with_quotes("SPY", DAY)
    root = fixture_lake.build()
    return root, mirror_lake(root, root.parent / "ssd")


def test_matched_names_every_file_the_walk_found_present_and_matching(fixture_lake):
    root, target = _backed_up(fixture_lake)
    shas = _shas(root)
    result = backup_scrub(root, target)
    assert result.matched == ((CHAINS, shas[CHAINS][-1]), (QUOTES, shas[QUOTES][-1]))
    assert result.walked is True


def test_matched_is_sorted_by_path_whatever_order_the_manifest_holds(tmp_path):
    # Quotes are sealed first here, so the manifest lists them before chains and a walk
    # that kept the manifest's order would hand them over first.
    root = FixtureLake(tmp_path / "lake").with_quotes("SPY", DAY).with_chains("SPY", DAY).build()
    assert [rel for rel in _shas(root)] == [QUOTES, CHAINS]
    target = mirror_lake(root, tmp_path / "ssd")
    assert [rel for rel, _ in backup_scrub(root, target).matched] == [CHAINS, QUOTES]


def test_matched_leaves_out_a_file_the_walk_found_wrong_or_gone(fixture_lake):
    root, target = _backed_up(fixture_lake)
    shas = _shas(root)
    (target / CHAINS).write_bytes(b"rot")
    result = backup_scrub(root, target)
    assert result.sha_mismatches == (CHAINS,)
    assert result.matched == ((QUOTES, shas[QUOTES][-1]),)
    # The walk still reached the end, so the restore still runs on what matched.
    assert result.walked is True

    (target / QUOTES).unlink()
    result = backup_scrub(root, target)
    assert result.missing == (QUOTES,)
    assert result.matched == ()
    assert result.walked is True


def test_matched_carries_the_sha_at_the_watermark_not_the_latest_one(fixture_lake):
    # The copy was taken before the lake rewrote the partition, so what the copy should
    # hold is the older sha. Handing over the newer one would read a sound copy as rot.
    root, target = _backed_up(fixture_lake)
    row = dict(sample_chains_table().to_pylist()[0])
    two_rows = [row, {**row, "occ_symbol": "SPY   260918C00655000"}]
    fixture_lake.with_chains("SPY", DAY, sample_chains_table(two_rows)).build()
    shas = _shas(root)
    assert len(shas[CHAINS]) == 2 and shas[CHAINS][0] != shas[CHAINS][1]

    result = backup_scrub(root, target)
    assert dict(result.matched)[CHAINS] == shas[CHAINS][0]


def test_a_partition_sealed_after_the_last_sync_is_not_matched(fixture_lake):
    root, target = _backed_up(fixture_lake)
    later = "chains/ticker=SPY/date=2026-08-31.parquet"
    fixture_lake.with_chains("SPY", date(2026, 8, 31)).build()
    result = backup_scrub(root, target)
    assert later in result.pending
    assert later not in dict(result.matched)


def test_a_scrub_that_stopped_early_matched_nothing_and_did_not_walk(fixture_lake, tmp_path):
    root, target = _backed_up(fixture_lake)
    unmounted = backup_scrub(root, tmp_path / "not-there")
    assert unmounted.target_missing is True
    assert unmounted.walked is False and unmounted.matched == ()


@pytest.mark.parametrize(
    "stop",
    [
        {"target_missing": True},
        {"manifest_missing": True},
        {"manifest_diverged_at": 0},
        {"unreadable": "OSError: gone"},
        {"bucket_refused": "AccessDenied"},
        {"bucket_unreachable": "EndpointConnectionError"},
        {"bucket_failed": "NoSuchBucket"},
        {"bucket_unusable": "bucket_region is not set"},
    ],
)
def test_each_stopping_finding_means_the_walk_did_not_finish(stop):
    assert BackupScrubResult(target="/ssd", **stop).walked is False


def test_findings_that_do_not_stop_the_walk_leave_it_walked():
    walked = BackupScrubResult(
        target="/ssd",
        missing=("a",),
        sha_mismatches=("b",),
        unaccounted=("c",),
        orphans=("d",),
        pending=("e",),
    )
    assert walked.walked is True
    assert BackupScrubResult(target="/ssd").walked is True
