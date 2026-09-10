"""The backup's exclusion list.

The design pins the sync root as ``lake/`` only, with an explicit exclusion list.
``runner.BACKUP_EXCLUSIONS`` is that list. These tests cover three things about it.

1. Every pattern reaches ``rsync``, ahead of the source and target operands.
2. The list is exactly the two justified entries, each derived from the constant that
   also spells the thing it names. A drifted spelling fails here rather than silently
   putting a temp file or a credential on the backup disk.
3. The patterns drop the temp file and the config directory, and nothing else. An
   over-broad pattern is the dangerous failure. It drops real data, the sync still
   exits clean, and the loss surfaces only at a restore. So the list is run against a
   fixture lake holding one of everything the design's lake tree names.

No real ``rsync`` runs. ``RsyncBackup`` takes its command runner as a seam, so a fake
records the argument list instead of copying anything. That is the same shape as the
``BackupRunner`` fake one layer up, which the orchestration tests inject.
"""

from __future__ import annotations

import fnmatch
from datetime import date
from pathlib import Path

import pytest

from lake import compact
from lake.paths import CONFIG_DIR_PARTS, CONFIG_FILE, TEMP_MARKER, LakePaths, temp_write_path
from lake.runner import BACKUP_EXCLUSIONS, BackupTargetUnavailable, RsyncBackup
from tests.support.lake import FixtureLake, sample_chains_table, sample_quotes_table

DAY = date(2026, 8, 24)


class RecordingRun:
    """A command runner that records the argument list rather than running it."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def __call__(self, args: list[str]) -> None:
        self.calls.append(list(args))


def _sync(tmp_path: Path, **kwargs) -> list[str]:
    """Run one ``RsyncBackup.sync`` against a recorder and return the argument list."""
    source = tmp_path / "lake"
    target = tmp_path / "ssd"
    source.mkdir()
    target.mkdir()
    run = RecordingRun()
    RsyncBackup(run=run, **kwargs).sync(source, target)
    assert len(run.calls) == 1
    return run.calls[0]


# -- 1. the patterns reach rsync ---------------------------------------------


def test_every_exclusion_reaches_rsync_once(tmp_path):
    args = _sync(tmp_path)
    excludes = [arg for arg in args if arg.startswith("--exclude")]
    assert excludes == [f"--exclude={pattern}" for pattern in BACKUP_EXCLUSIONS]


def test_the_exclusions_precede_the_source_and_target_operands(tmp_path):
    args = _sync(tmp_path)
    # The operands are the last two arguments. A pattern after them would be read as a
    # third operand, which rsync takes as another source to copy.
    last_exclude = max(index for index, arg in enumerate(args) if arg.startswith("--exclude"))
    assert last_exclude < len(args) - 2
    assert args[-2].endswith("/lake/") and args[-1].endswith("/ssd/")


def test_extra_args_cannot_land_between_the_exclusions(tmp_path):
    args = _sync(tmp_path, extra_args=("--dry-run",))
    excludes = [index for index, arg in enumerate(args) if arg.startswith("--exclude")]
    # Contiguous, and all of them before the caller's own flag.
    assert excludes == list(range(excludes[0], excludes[0] + len(BACKUP_EXCLUSIONS)))
    assert excludes[-1] < args.index("--dry-run")


def test_an_unmounted_target_raises_before_any_command_is_built(tmp_path):
    run = RecordingRun()
    with pytest.raises(BackupTargetUnavailable):
        RsyncBackup(run=run).sync(tmp_path / "lake", tmp_path / "never-mounted")
    assert run.calls == []


# -- 2. the list is the two justified entries --------------------------------


def test_the_list_is_exactly_the_temp_marker_and_the_config_directory():
    assert BACKUP_EXCLUSIONS == (
        f"*{TEMP_MARKER}*",
        "/".join(CONFIG_DIR_PARTS) + "/",
    )


def test_the_temp_pattern_matches_what_the_shared_helper_builds(tmp_path):
    # The pattern is derived from the same constant ``temp_write_path`` uses. This
    # checks that the derivation lands on the real name, not merely on the constant.
    tmp = temp_write_path(tmp_path / "date=2026-08-24.parquet", 4242)
    assert _excluded(tmp.name)


def test_the_pattern_catches_the_temp_the_partition_writer_uses(tmp_path, monkeypatch):
    """Bind the exclusion to the production writer, not only to the shared helper.

    ``_write_partition`` renames its temp away on success and unlinks it on any
    catchable failure. So real debris needs a hard kill and cannot be staged here.
    What can be staged is the name. Wrapping the Parquet write records the path the
    writer hands it, which is the file a killed writer would leave behind. A writer
    that spelled its own temp name instead of asking ``temp_write_path`` fails here.
    """
    written: list[Path] = []
    real_write_table = compact.pq.write_table

    def record(table, where, *args, **kwargs):
        written.append(Path(where))
        return real_write_table(table, where, *args, **kwargs)

    monkeypatch.setattr(compact.pq, "write_table", record)
    compact._write_partition(sample_chains_table(), tmp_path / "date=2026-08-24.parquet")

    assert len(written) == 1
    assert _excluded(written[0].name)


def test_nothing_the_design_keeps_in_the_sync_root_is_named():
    # The four surfaces, the journal, the two ledgers, the reference tables, and the
    # reports directory. Each is pinned by the design as inside the sync root.
    kept = (
        "chains",
        "quotes",
        "bars",
        "actions",
        "journal",
        "reference",
        "reports",
        "manifest.jsonl",
        "quarantine.jsonl",
    )
    for pattern in BACKUP_EXCLUSIONS:
        assert not any(name in pattern for name in kept)


# -- 3. the patterns drop those two things and nothing else ------------------


def _excluded(rel: str, patterns: tuple[str, ...] = BACKUP_EXCLUSIONS) -> bool:
    """Whether ``rsync`` would exclude the file at lake-relative path ``rel``.

    This models the two pattern shapes ``BACKUP_EXCLUSIONS`` uses and no others.

    1. A pattern holding no "/" is matched by rsync against a path's last component.
       Since rsync considers each directory on the way down as well as the file, the
       pattern drops the name wherever in the tree it appears. So every component is
       tested.
    2. A pattern holding a "/" is matched against the end of the path, and a trailing
       "/" narrows the match to directories. So it is tested against each of the path's
       directory prefixes. The argument is always a file path here, which is why the
       last component is never offered as a directory.

    A pattern in neither shape fails the test rather than being waved through. An
    approximate matcher that silently passed an unmodelled pattern would be worse than
    no matcher, because the over-breadth check below would go quietly green.
    """
    components = rel.split("/")
    for pattern in patterns:
        if "/" not in pattern:
            if any(fnmatch.fnmatchcase(part, pattern) for part in components):
                return True
            continue
        if not pattern.endswith("/") or pattern.startswith("/") or "*" in pattern or "?" in pattern:
            # A leading "/" anchors the pattern to the transfer root, which this matcher
            # does not model. Without this arm it passes the shape check and then
            # `rstrip("/").split("/")` yields a leading empty component that no relative
            # path can ever equal, so the matcher answers "not excluded" for a pattern
            # rsync applies to the whole subtree. That is the silent pass the docstring
            # above promises never to give.
            raise AssertionError(f"exclusion shape this matcher does not model: {pattern!r}")
        wanted = pattern.rstrip("/").split("/")
        for end in range(len(wanted), len(components)):
            if components[end - len(wanted) : end] == wanted:
                return True
    return False


def _full_lake(root: Path) -> Path:
    """A lake holding one of everything the design's lake tree names."""
    lake = (
        FixtureLake(root)
        .with_chains("SPY", DAY)
        .with_quotes("SPY", DAY)
        .with_partition("bars", "SPY", DAY, sample_quotes_table())
        .with_reference("security_master", sample_chains_table())
        .with_reference("contracts", sample_chains_table())
        .with_journal_segment(
            "chains", "SPY", DAY, sample_chains_table(), start_ts="20260824T133000Z", pid=4242
        )
        .with_quarantine({"partition": "chains/ticker=SPY/date=2026-08-24.parquet"})
        .build()
    )
    paths = LakePaths(lake)
    # The two shapes ``FixtureLake`` does not build: the all-ticker corporate actions
    # file and a dated nightly report. Both are named by the design and both sit inside
    # the sync root, so both belong in an over-breadth check.
    paths.actions_path.parent.mkdir(parents=True, exist_ok=True)
    paths.actions_path.write_bytes(b"parquet")
    report = lake / "reports" / f"date={DAY.isoformat()}.md"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text("nightly report\n")
    return lake


def _relative_files(root: Path) -> list[str]:
    return sorted(p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file())


def test_no_pattern_drops_a_file_a_real_lake_holds(tmp_path):
    lake = _full_lake(tmp_path / "lake")
    files = _relative_files(lake)
    # Guard the guard. An empty listing would pass the assertion below saying nothing.
    assert len(files) > 8
    assert [rel for rel in files if _excluded(rel)] == []


def test_a_crashed_writers_temp_file_is_dropped(tmp_path):
    lake = _full_lake(tmp_path / "lake")
    partition = LakePaths(lake).chains_partition_path("SPY", DAY)
    debris = temp_write_path(partition, 4242)
    debris.write_bytes(b"half a partition")

    dropped = [rel for rel in _relative_files(lake) if _excluded(rel)]
    assert dropped == [debris.relative_to(lake).as_posix()]


def test_the_config_directory_is_dropped_wherever_it_sits_under_the_root(tmp_path):
    # The directory is outside the sync root today, so this pattern matches nothing in
    # a real lake. The exclusion exists for the day the sync root widens, and this is
    # that day simulated: the credential directory placed inside the root.
    lake = _full_lake(tmp_path / "lake")
    config = lake.joinpath(*CONFIG_DIR_PARTS)
    config.mkdir(parents=True)
    (config / "token.json").write_text("{}\n")
    (config / CONFIG_FILE).write_text("lake_root: /lake\n")

    dropped = [rel for rel in _relative_files(lake) if _excluded(rel)]
    assert dropped == [
        f"{'/'.join(CONFIG_DIR_PARTS)}/{CONFIG_FILE}",
        f"{'/'.join(CONFIG_DIR_PARTS)}/token.json",
    ]


def test_the_journal_survives_the_exclusions(tmp_path):
    # The single-copy window the backup exists to close. Between a capture and the
    # close+15 seal, the journal segment is the day's only copy. An exclusion that
    # dropped it would make the backup attest a day it never carried.
    lake = _full_lake(tmp_path / "lake")
    segments = [rel for rel in _relative_files(lake) if rel.startswith("journal/")]
    assert segments and not any(_excluded(rel) for rel in segments)


def test_the_matcher_refuses_a_shape_it_does_not_model():
    """The over-breadth guard is only worth having if it cannot go quietly green.

    A leading "/" anchors a pattern to the transfer root. This matcher does not model
    that, and before the guard rejected it the pattern passed the shape check and then
    produced a leading empty component no relative path can equal. The matcher answered
    "not excluded" for a pattern rsync applies to the whole subtree, so the over-breadth
    tests below would have gone green while a real sync dropped the journal.
    """
    segment = "journal/date=2026-08-24/surface=chains/ticker=SPY/seg-1.arrows"

    # The unanchored form is modelled, and it does exclude the segment.
    assert _excluded(segment, ("journal/",))

    # The anchored form is refused rather than mismatched.
    with pytest.raises(AssertionError, match="does not model"):
        _excluded(segment, ("/journal/",))
