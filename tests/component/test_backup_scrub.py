"""The backup-copy scrub, across one real boundary: the filesystem.

``manifest.backup_scrub`` walks an rsync target and checks it against the lake's own
manifest. These tests build a lake with the fixture builder, copy it the way a clean
sync would leave it, and then damage one side or the other. No ``rsync`` runs, and none
could. ``tests/conftest.py`` fails any test that shells out to one.

Four groups, one per question the scrub has to answer.

1. Rot and loss on the copy. A file whose bytes changed, and a file that is gone. This
   is the coverage that let ``rsync --checksum`` be dropped, so it is the group that
   matters most.
2. Which side diverged. The lake is the authority, so a lake that is clean and a copy
   that is not says the copy rotted, and the primary scrub in the same Sunday run is
   what says the other case.
3. Staleness. A backup is behind its lake between one sync and the next, and a check
   that called that loss would cry wolf every week.
4. The copy's own manifest. It is read for its length and never trusted as the
   authority, and a copy that rotted its way into agreeing with itself is reported
   rather than believed.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

from lake.manifest import backup_scrub, scrub
from lake.paths import MANIFEST_FILE
from tests.support.backup import mirror_lake
from tests.support.lake import FixtureLake, sample_chains_table

DAY = date(2026, 8, 28)
LATER = date(2026, 8, 31)
CHAINS = f"chains/ticker=SPY/date={DAY.isoformat()}.parquet"


def _lake(fixture_lake: FixtureLake) -> Path:
    """A two-partition lake, the shape every test here starts from."""
    fixture_lake.with_chains("SPY", DAY)
    fixture_lake.with_quotes("SPY", DAY)
    return fixture_lake.build()


def _backed_up(fixture_lake: FixtureLake) -> tuple[Path, Path]:
    """That lake, and a clean copy of it beside it."""
    root = _lake(fixture_lake)
    return root, mirror_lake(root, root.parent / "ssd")


def _two_rows() -> list[dict]:
    """Two chains rows, so a rewritten partition really does hold different bytes.

    Handing ``with_chains`` an empty table would not do it. A pyarrow table with no rows
    is falsy, so the builder's ``table or sample_chains_table()`` quietly swaps the
    default one-row table back in and the file lands unchanged.
    """
    row = dict(sample_chains_table().to_pylist()[0])
    return [row, {**row, "occ_symbol": "SPY   260918C00655000"}]


def _listing(root: Path) -> dict[str, str]:
    """Every file under a tree, by relative path, with the sha of its bytes."""
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file()
    }


# -- 1. rot and loss on the copy ---------------------------------------------


def test_a_clean_copy_scrubs_clean(fixture_lake):
    root, target = _backed_up(fixture_lake)
    result = backup_scrub(root, target)
    assert result.ok
    assert result.problem is None
    assert result.missing == () and result.sha_mismatches == () and result.orphans == ()
    assert result.pending == ()


def test_a_rotted_file_on_the_copy_is_named(fixture_lake):
    # The failure ``--checksum`` was standing in for: bytes that changed on the target
    # with nothing else looking. The flag re-copied over it and exited clean. This names
    # the file instead.
    root, target = _backed_up(fixture_lake)
    (target / CHAINS).write_bytes(b"rot")

    result = backup_scrub(root, target)
    assert result.sha_mismatches == (CHAINS,)
    assert result.missing == () and result.orphans == ()
    assert result.ok is False
    # The count decides the ping and the path says where to look, so both surface, and
    # the line names the disk it came from.
    assert result.problem == (
        f"backup scrub failed: missing=0 sha_mismatches=1 unaccounted=0: {target}"
    )
    assert f"backup file does not match the lake: {CHAINS}" in result.notes


def test_rot_that_keeps_the_size_is_still_caught(fixture_lake):
    # Bit rot is what the flag existed for, and its signature is bytes that changed
    # while size and mtime did not. A size-and-mtime sync cannot see this, which is why
    # the scrub compares shas rather than asking rsync to.
    root, target = _backed_up(fixture_lake)
    path = target / CHAINS
    before = path.stat()
    flipped = bytearray(path.read_bytes())
    flipped[-1] ^= 0xFF
    path.write_bytes(bytes(flipped))
    import os

    os.utime(path, (before.st_atime, before.st_mtime))

    assert path.stat().st_size == before.st_size
    assert path.stat().st_mtime == before.st_mtime
    assert backup_scrub(root, target).sha_mismatches == (CHAINS,)


def test_a_file_missing_from_the_copy_is_named(fixture_lake):
    root, target = _backed_up(fixture_lake)
    (target / CHAINS).unlink()

    result = backup_scrub(root, target)
    assert result.missing == (CHAINS,)
    assert result.sha_mismatches == () and result.orphans == ()
    assert result.ok is False
    assert result.problem == (
        f"backup scrub failed: missing=1 sha_mismatches=0 unaccounted=0: {target}"
    )
    assert f"backup file gone: {CHAINS}" in result.notes


def test_a_file_on_the_copy_with_no_entry_is_an_orphan(fixture_lake):
    # A killed rsync leaves its hidden temp behind. The design puts that orphan on the
    # backup scrub rather than on rsync, because the flag only ever compared paths that
    # exist on both sides.
    root, target = _backed_up(fixture_lake)
    debris = target / "chains" / "ticker=SPY" / ".date=2026-08-28.parquet.YXbQ1f"
    debris.write_bytes(b"half a transfer")

    result = backup_scrub(root, target)
    rel = debris.relative_to(target).as_posix()
    assert result.orphans == (rel,)
    assert result.missing == () and result.sha_mismatches == ()
    # Named, never paged. An extra file on the copy costs space rather than data, and
    # macOS writes files to a mounted volume without anyone asking, so an orphan that
    # withheld the ping would page on a folder someone opened in Finder.
    assert result.ok is True and result.problem is None
    assert f"backup file the lake never recorded: {rel}" in result.notes


def test_the_reverse_pass_skips_what_the_lake_s_own_scrub_skips(fixture_lake):
    # One exclusion list, not two. A file that is not an orphan on the primary is not
    # one on the copy either, so a decision about that list is made once.
    root, target = _backed_up(fixture_lake)
    for rel in (
        "reports/date=2026-08-28.md",
        "journal/date=2026-08-28/surface=chains/ticker=SPY/seg-1.arrows",
    ):
        path = target / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"not a partition")

    assert backup_scrub(root, target).ok


def test_the_scrub_writes_nothing_to_either_tree(fixture_lake):
    # Read-only, the same promise ``scrub`` carries. Repair is a separate, deliberate,
    # human-invoked step, and a scrub that quietly healed the copy would destroy the
    # evidence that the disk is failing.
    root, target = _backed_up(fixture_lake)
    (target / CHAINS).write_bytes(b"rot")
    before_lake, before_target = _listing(root), _listing(target)

    assert backup_scrub(root, target).ok is False
    assert _listing(root) == before_lake
    assert _listing(target) == before_target


# -- 2. which side diverged --------------------------------------------------


def test_a_clean_lake_and_a_rotted_copy_leave_the_primary_scrub_clean(fixture_lake):
    root, target = _backed_up(fixture_lake)
    (target / CHAINS).write_bytes(b"rot")

    assert scrub(root).ok
    assert backup_scrub(root, target).sha_mismatches == (CHAINS,)


def test_a_rotted_lake_leaves_the_copy_clean_so_the_pair_names_the_side(fixture_lake):
    # The other direction, and the reason the Sunday job runs both. The manifest is the
    # authority, so a partition that rotted on the primary fails the primary scrub while
    # the copy still matches what the ledger recorded. One scrub speaking and the other
    # silent is what names the side. ``rsync --checksum`` ran the opposite way: it took
    # the source as correct, copied the rot over the last good backup, and exited clean.
    root, target = _backed_up(fixture_lake)
    (root / CHAINS).write_bytes(b"rot")

    assert scrub(root).sha_mismatches == (CHAINS,)
    assert backup_scrub(root, target).ok


# -- 3. staleness ------------------------------------------------------------


def test_a_partition_sealed_since_the_copy_is_pending_and_not_loss(fixture_lake):
    # The edge that decides whether this check is worth having. The backup is written
    # at close+15 and scrubbed on Sunday, so a day sealed in between is legitimately
    # absent. Calling that loss would page every week.
    root, target = _backed_up(fixture_lake)
    fixture_lake.with_chains("SPY", LATER)
    fixture_lake.build()
    sealed = f"chains/ticker=SPY/date={LATER.isoformat()}.parquet"

    result = backup_scrub(root, target)
    assert result.ok
    assert result.missing == ()
    assert result.pending == (sealed,)
    assert not (target / sealed).exists()


def test_a_partition_superseded_since_the_copy_is_judged_at_the_copy_s_own_watermark(
    fixture_lake,
):
    # A recompaction appends a second entry for a path already on the backup. The copy
    # still holds the older bytes, which is correct for the sync that made it, so the
    # expected sha is the one recorded at or before the watermark rather than the
    # newest one.
    root, target = _backed_up(fixture_lake)
    older = (target / CHAINS).read_bytes()
    fixture_lake.with_chains("SPY", DAY, sample_chains_table(_two_rows()))
    fixture_lake.build()

    assert (root / CHAINS).read_bytes() != older
    result = backup_scrub(root, target)
    assert result.ok
    assert result.sha_mismatches == () and result.pending == ()


def test_a_segment_the_copy_still_holds_is_checked_against_its_own_entry(fixture_lake):
    # Compaction merges a ticker-day, appends the compacted entry, then unlinks the
    # segments. A copy taken before that still carries the segment, and its entry is
    # inside the watermark with no compacted entry beside it, so the segment is checked
    # and is there.
    fixture_lake.with_journal_segment(
        "chains", "SPY", DAY, sample_chains_table(), start_ts="20260828T133000Z", pid=4242
    )
    root = _lake(fixture_lake)
    segment = fixture_lake.segment_path("chains", "SPY", DAY, "20260828T133000Z", 4242)
    rel = segment.relative_to(root).as_posix()
    _append(root, rel, segment)
    target = mirror_lake(root, root.parent / "ssd")

    result = backup_scrub(root, target)
    assert result.ok
    assert (target / rel).exists()


def test_a_segment_compacted_before_the_copy_is_not_missing_from_it(fixture_lake):
    # The other order. Both entries are inside the watermark, so the segment entry is
    # superseded and its absence from the copy is compaction's work, not loss. This is
    # the same supersession rule the primary scrub uses.
    fixture_lake.with_journal_segment(
        "chains", "SPY", DAY, sample_chains_table(), start_ts="20260828T133000Z", pid=4242
    )
    root = _lake(fixture_lake)
    segment = fixture_lake.segment_path("chains", "SPY", DAY, "20260828T133000Z", 4242)
    rel = segment.relative_to(root).as_posix()
    _append(root, rel, segment)
    segment.unlink()
    target = mirror_lake(root, root.parent / "ssd")

    assert scrub(root).ok
    assert backup_scrub(root, target).ok


def _append(root: Path, partition: str, path: Path) -> None:
    """Append one manifest entry for a file the fixture builder wrote without one."""
    entry = {
        "partition": partition,
        "source": "capture",
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "rows": 1,
        "fetched_at": None,
    }
    with (root / MANIFEST_FILE).open("a") as handle:
        handle.write(json.dumps(entry, sort_keys=True) + "\n")


# -- 4. the copy's own manifest ----------------------------------------------


def test_a_missing_target_is_reported_rather_than_read_as_clean(fixture_lake):
    root = _lake(fixture_lake)
    target = root.parent / "never-mounted"

    result = backup_scrub(root, target)
    assert result.target_missing is True
    assert result.ok is False
    assert result.problem == f"backup target not mounted: {target}"


def test_a_target_with_no_manifest_copy_is_reported_rather_than_read_as_empty(fixture_lake):
    # An SSD that is mounted and holds nothing. Every partition would be past a
    # watermark of zero, so without this the run would come back clean while no sync
    # had ever landed.
    root = _lake(fixture_lake)
    target = root.parent / "ssd"
    target.mkdir()

    result = backup_scrub(root, target)
    assert result.manifest_missing is True
    assert result.ok is False
    assert result.problem == f"backup carries no usable manifest copy: {target}"


def test_a_rotted_manifest_copy_is_named_by_byte_offset(fixture_lake):
    root, target = _backed_up(fixture_lake)
    copy = target / MANIFEST_FILE
    original = copy.read_bytes()
    flipped = bytearray(original)
    flipped[4] ^= 0xFF
    copy.write_bytes(bytes(flipped))

    result = backup_scrub(root, target)
    assert result.manifest_diverged_at == 4
    assert result.ok is False
    assert f"diverged from the lake's at byte 4: {target}" in result.problem


def test_a_manifest_copy_that_diverges_late_is_caught_too(fixture_lake):
    # The first entry is the easy case and the only one a naive check holds. Rot lands
    # wherever it lands, so the check has to reach the whole file.
    root, target = _backed_up(fixture_lake)
    copy = target / MANIFEST_FILE
    original = copy.read_bytes()
    offset = original.index(b"\n") + 30
    flipped = bytearray(original)
    flipped[offset] ^= 0xFF
    copy.write_bytes(bytes(flipped))

    assert backup_scrub(root, target).manifest_diverged_at == offset


def test_a_manifest_copy_longer_than_the_lake_s_is_not_a_clean_prefix(fixture_lake):
    # A lake restored or rebuilt from behind, beside an older copy. The copy carries
    # ledger lines the lake does not, so the watermark means nothing and reading the
    # copy as merely ahead would scrub against a ledger the lake never wrote.
    root, target = _backed_up(fixture_lake)
    source = root / MANIFEST_FILE
    end = len(source.read_bytes())
    with (target / MANIFEST_FILE).open("a") as handle:
        handle.write(json.dumps({"partition": "chains/ticker=IWM/date=2026-08-28.parquet"}) + "\n")

    result = backup_scrub(root, target)
    assert result.manifest_diverged_at == end
    assert result.ok is False


def test_rot_that_breaks_a_line_s_json_does_not_switch_the_scrub_off(fixture_lake):
    """The hole a parsed-entry comparison leaves, closed by comparing bytes.

    ``parse_jsonl`` discards the first line it cannot parse and every line after it,
    because the append rule says only the last line can be torn. Rot on an SSD obeys no
    such rule. Read through that parser, one wrecked byte in the copy's first line would
    discard the whole tail, collapse the watermark to zero, and let a backup whose every
    file had rotted come back clean.
    """
    root, target = _backed_up(fixture_lake)
    copy = target / MANIFEST_FILE
    wrecked = bytearray(copy.read_bytes())
    wrecked[2] = 0x00
    copy.write_bytes(bytes(wrecked))
    for path in target.rglob("*.parquet"):
        path.write_bytes(b"rot")

    result = backup_scrub(root, target)
    assert result.manifest_diverged_at == 2
    assert result.ok is False


def test_a_copy_that_agrees_with_itself_and_not_with_the_lake_is_reported(fixture_lake):
    # The case that decides which manifest is the authority. Rot the file and rewrite
    # the copy's own manifest to match it, and a scrub against that copy passes while
    # the backup no longer holds what the lake holds. Against the lake's manifest it
    # cannot pass.
    root, target = _backed_up(fixture_lake)
    (target / CHAINS).write_bytes(b"rot")
    lines = []
    for line in (target / MANIFEST_FILE).read_text().splitlines():
        entry = json.loads(line)
        if entry["partition"] == CHAINS:
            entry["sha256"] = hashlib.sha256(b"rot").hexdigest()
        lines.append(json.dumps(entry, sort_keys=True))
    (target / MANIFEST_FILE).write_text("\n".join(lines) + "\n")

    # Self-consistent: the copy matches its own ledger.
    assert scrub(target).ok
    # And reported all the same, because the lake's ledger is what decides.
    assert backup_scrub(root, target).ok is False


def test_a_torn_trailing_line_is_not_divergence_and_the_copy_still_says_so(fixture_lake):
    """A prefix that stops mid-line is still a prefix, so it is not divergence.

    The entry whose line was torn drops out of the watermark, so its partition reads as
    pending. Its file is on the copy all the same, because a file is written before its
    entry is appended, and that is ``unaccounted``: the copy holding something its own
    manifest copy does not reach. Both readings say the one true thing, which is that
    this copy's manifest is shorter than the copy that was taken. Under the compaction
    job's lock no append is ever in flight, so the sync that writes the real backup
    never sees a torn line at all.
    """
    root, target = _backed_up(fixture_lake)
    text = (target / MANIFEST_FILE).read_text()
    torn = json.loads(text.splitlines()[-1])["partition"]
    (target / MANIFEST_FILE).write_text(text[: text.rindex("\n") - 10])

    result = backup_scrub(root, target)
    assert result.manifest_diverged_at is None
    assert result.pending == (torn,)
    assert result.unaccounted == (torn,)
    assert result.ok is False


def test_a_manifest_copy_truncated_to_nothing_is_not_a_clean_watermark(fixture_lake):
    # A watermark of zero over a lake that has entries would put every partition past it
    # and leave nothing for the scrub to check, which is the quietest way for this whole
    # feature to stop working.
    root, target = _backed_up(fixture_lake)
    (target / MANIFEST_FILE).write_bytes(b"")

    result = backup_scrub(root, target)
    assert result.manifest_missing is True
    assert result.ok is False


def test_a_plain_file_where_the_target_should_be_is_not_a_mounted_disk(fixture_lake):
    # The repair for an unmounted disk and the repair for a disk carrying no backup are
    # different, so the finding has to tell them apart.
    root = _lake(fixture_lake)
    target = root.parent / "ssd"
    target.write_bytes(b"not a mount point")

    result = backup_scrub(root, target)
    assert result.target_missing is True
    assert result.problem == f"backup target not mounted: {target}"


def test_a_read_that_fails_is_a_named_finding_rather_than_a_raise(fixture_lake):
    """The scrub reads the one disk in this system built to fail.

    Every other Sunday check runs after it, so a raise would cost the run its canary,
    its coverage assertion and its re-auth reminder, and report a traceback in place of
    the disk. A bad sector, or the cable pulled mid-walk, is a named finding instead.
    """
    root, target = _backed_up(fixture_lake)
    (target / CHAINS).chmod(0o000)
    try:
        result = backup_scrub(root, target)
    finally:
        (target / CHAINS).chmod(0o644)

    assert result.unreadable is not None
    assert result.ok is False
    assert result.problem.startswith("backup could not be read: PermissionError")


def test_many_wrong_files_name_a_few_and_then_say_how_many(fixture_lake):
    # A disk going bad names every file it carries. One report line per partition would
    # bury every other finding the run made, so the naming stops and counts the rest.
    for ticker in ("SPY", "QQQ", "IWM", "DIA", "XLF"):
        fixture_lake.with_chains(ticker, DAY)
    root = fixture_lake.build()
    target = mirror_lake(root, root.parent / "ssd")
    for path in target.glob("chains/**/*.parquet"):
        path.write_bytes(b"rot")

    result = backup_scrub(root, target)
    assert len(result.sha_mismatches) == 5
    named = [line for line in result.notes if "does not match" in line]
    assert len(named) == 4
    assert named[-1] == "backup file does not match the lake: and 2 more"
