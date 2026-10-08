"""The trimmed ledger on real files: its strict append, its manifest entry, and the scrub.

Marketlake #782. Every writer of ``trimmed.jsonl`` holds the lake-root lock, so its append can
refuse a torn tail and read its line back, which ``manifest.append_line`` cannot. The Sunday
scrub reads the ledger to tell a partition removed on purpose from one lost, and reads it only
when it exists, so a lake that never trims scrubs exactly as before.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from lake import trimmed
from lake.lock import lake_lock
from lake.manifest import (
    LedgerHasByteOrderMark,
    LedgerNotUtf8,
    ScrubResult,
    TornLedger,
    latest_entries,
    record_partition,
    scrub,
    sha256_file,
)
from lake.paths import TRIMMED_FILE
from lake.trimmed import (
    TrimmedLineLost,
    TrimmedRepairRefused,
    TrimmedTornTail,
    append_trimmed,
    read_trimmed,
    refresh_trimmed_entry,
    repair_trimmed_entry,
    restore_line,
    trim_line,
    trimmed_path,
)
from tests.support.lake import FixtureLake

DAY = date(2026, 9, 1)
NEXT = date(2026, 9, 2)
SPY = f"chains/ticker=SPY/date={DAY.isoformat()}.parquet"
SPY_NEXT = f"chains/ticker=SPY/date={NEXT.isoformat()}.parquet"
SOURCE = "test-trim"
STAMP = "2026-10-07T16:41:00-04:00"


def _lake(root: Path) -> Path:
    return FixtureLake(root).with_chains("SPY", DAY).with_chains("SPY", NEXT).build()


def _trim(partition: str, sha256: str) -> dict:
    return trim_line(partition, sha256=sha256, version_id="v1", verified_at=STAMP, trimmed_at=STAMP)


def _append(root: Path, line: dict) -> dict:
    with lake_lock(root):
        return append_trimmed(root, line, source=SOURCE, fetched_at=STAMP)


def _trim_away(root: Path, partition: str) -> None:
    """Remove a partition the way #787's trim will: the line, its entry, then the unlink."""
    _append(root, _trim(partition, latest_entries(root)[partition]["sha256"]))
    (root / partition).unlink()


# -- the strict append ---------------------------------------------------------


def test_an_append_lands_one_line_and_reads_it_back(tmp_path):
    root = _lake(tmp_path / "lake")
    line = _trim(SPY, "a" * 64)

    assert _append(root, line) == line
    assert read_trimmed(root) == [line]
    assert trimmed_path(root).read_bytes() == (json.dumps(line, sort_keys=True) + "\n").encode()


def test_a_torn_tail_refuses_the_append_and_leaves_the_file_unchanged(tmp_path):
    """Mutation this catches: removing the tail check, which lets the new line fuse.

    Without the check the line would fuse onto the fragment, the read-back would then refuse
    with the other class, and the file would have grown. Both the class and the unchanged bytes
    are asserted, so either symptom of the mutation fails here.
    """
    root = _lake(tmp_path / "lake")
    _append(root, _trim(SPY, "a" * 64))
    with trimmed_path(root).open("ab") as handle:
        handle.write(b'{"kind": "trim", "partition": "chains/ti')
    before = trimmed_path(root).read_bytes()
    entry_before = latest_entries(root)[TRIMMED_FILE]

    with pytest.raises(TrimmedTornTail) as refused:
        _append(root, _trim(SPY_NEXT, "b" * 64))

    assert trimmed_path(root).read_bytes() == before
    assert latest_entries(root)[TRIMMED_FILE] == entry_before
    message = str(refused.value)
    assert "no terminating newline" in message and "Repair by hand under the lock" in message
    assert "\n" not in message


def test_a_write_that_loses_bytes_fails_the_read_back(tmp_path, monkeypatch):
    """Mutation this catches: removing the read-back.

    The fault is injected at the one write call, and it reports every byte written while landing
    all but the last few. ``_append_once`` checks only the count the kernel reports, so this is
    the case the read-back exists for: nothing else notices, and the caller would unlink.
    """
    root = _lake(tmp_path / "lake")
    real = trimmed._append_once

    def lossy(path, data):
        real(path, data[:-5] + b"\n")

    monkeypatch.setattr(trimmed, "_append_once", lossy)
    entry_before = latest_entries(root).get(TRIMMED_FILE)

    with pytest.raises(TrimmedLineLost) as refused:
        _append(root, _trim(SPY, "a" * 64))

    assert latest_entries(root).get(TRIMMED_FILE) == entry_before, "no entry certifies it"
    assert SPY in str(refused.value)


@pytest.mark.parametrize(
    "garbled",
    [
        pytest.param(b'{"kind": "trim", "partition": "chains/ti\n', id="lost bytes, kept newline"),
        pytest.param(b"[1, 2]\n", id="json that is not an object"),
        pytest.param(b"\xff\xfe\n", id="bytes that do not decode"),
    ],
)
def test_a_garbled_last_line_with_a_newline_refuses_the_append(tmp_path, garbled):
    """Mutation this catches: checking only for the trailing newline.

    A write that lost bytes mid-line and kept its newline ends in ``\\n``, so the newline check
    passes it, and ``parse_jsonl`` drops the line without a word. The append refuses before it
    writes, and the file is unchanged.
    """
    root = _lake(tmp_path / "lake")
    _append(root, _trim(SPY, "a" * 64))
    with trimmed_path(root).open("ab") as handle:
        handle.write(garbled + b"\n")
    before = trimmed_path(root).read_bytes()

    with pytest.raises(TrimmedTornTail) as refused:
        _append(root, _trim(SPY_NEXT, "b" * 64))

    assert trimmed_path(root).read_bytes() == before
    assert "does not parse as one JSON object" in str(refused.value)


def test_a_write_that_lands_nothing_fails_even_when_an_identical_line_is_last(
    tmp_path, monkeypatch
):
    """Mutation this catches: comparing only the last line, without the line count.

    The line being appended is already the ledger's last line, so a write that lands nothing
    leaves a last line equal to the new one. Only the count tells the two apart.
    """
    root = _lake(tmp_path / "lake")
    line = _trim(SPY, "a" * 64)
    _append(root, line)
    monkeypatch.setattr(trimmed, "_append_once", lambda path, data: None)
    entry_before = latest_entries(root)[TRIMMED_FILE]

    with pytest.raises(TrimmedLineLost):
        _append(root, line)

    assert latest_entries(root)[TRIMMED_FILE] == entry_before, "no entry certifies it"


def test_a_torn_partial_behind_an_identical_last_line_fails_the_read_back(tmp_path, monkeypatch):
    """A fragment with no newline is discarded by the reader, so the earlier line reads as last."""
    root = _lake(tmp_path / "lake")
    line = _trim(SPY, "a" * 64)
    _append(root, line)
    real = trimmed._append_once
    monkeypatch.setattr(trimmed, "_append_once", lambda path, data: real(path, data[:20]))
    entry_before = latest_entries(root)[TRIMMED_FILE]

    with pytest.raises(TrimmedLineLost):
        _append(root, line)

    assert latest_entries(root)[TRIMMED_FILE] == entry_before, "no entry certifies the tear"


def test_bytes_landed_behind_the_line_are_never_certified(tmp_path, monkeypatch):
    """Mutation this catches: dropping the tail check that runs after the read-back.

    The write lands the whole line and then a fragment. The read-back stops at the fragment, so
    it sees one new last line equal to this one and passes. ``_record`` checks no tail of its
    own, so without the second check the entry would certify the fragment.
    """
    root = _lake(tmp_path / "lake")
    real = trimmed._append_once
    monkeypatch.setattr(
        trimmed, "_append_once", lambda path, data: real(path, data + b'{"kind": "tr')
    )

    with pytest.raises(TrimmedTornTail):
        _append(root, _trim(SPY, "a" * 64))

    assert TRIMMED_FILE not in latest_entries(root), "no entry certifies the fragment"


def test_an_append_to_an_existing_empty_ledger_succeeds(tmp_path):
    """Mutation this catches: refusing a zero-byte ledger as torn.

    An empty file holds no line for the next one to fuse onto, so it is no torn tail.
    """
    root = _lake(tmp_path / "lake")
    trimmed_path(root).write_bytes(b"")
    line = _trim(SPY, "a" * 64)

    assert _append(root, line) == line
    assert read_trimmed(root) == [line]
    assert latest_entries(root)[TRIMMED_FILE]["rows"] == 1


@pytest.mark.parametrize(
    "tail",
    [
        pytest.param("whole line then blank", id="a blank line after a whole one"),
        pytest.param("blank only", id="only a blank line"),
    ],
)
def test_a_blank_last_line_is_no_torn_tail(tmp_path, tail):
    """Mutations this catches: judging the last line with blanks counted, and dropping the guard
    for a ledger of blank lines alone.

    ``parse_jsonl`` skips a blank line, so the ledger reads whole and the append has nothing to
    fuse onto. Counting the blank as the last line would refuse it as garbled, and a ledger with
    no non-blank line would index past the end.
    """
    root = _lake(tmp_path / "lake")
    first = _trim(SPY, "a" * 64)
    if tail == "whole line then blank":
        trimmed_path(root).write_text(json.dumps(first, sort_keys=True) + "\n\n")
        expected = [first]
    else:
        trimmed_path(root).write_text("\n")
        expected = []
    line = _trim(SPY_NEXT, "b" * 64)

    assert _append(root, line) == line
    assert read_trimmed(root) == [*expected, line]


# -- the ledger's own manifest entry ------------------------------------------


def test_each_append_refreshes_the_ledgers_entry_and_its_rows_grow_by_one(tmp_path):
    root = _lake(tmp_path / "lake")

    _append(root, _trim(SPY, "a" * 64))
    first = latest_entries(root)[TRIMMED_FILE]
    _append(root, restore_line(SPY, sha256="a" * 64, restored_at=STAMP))
    second = latest_entries(root)[TRIMMED_FILE]

    assert first["rows"] == 1
    assert second["rows"] == first["rows"] + 1
    assert second["sha256"] == sha256_file(trimmed_path(root))
    assert second["source"] == SOURCE and second["fetched_at"] == STAMP


def test_a_refresh_refuses_a_torn_tail_rather_than_certify_it(tmp_path):
    """Re-recording the entry over a torn last line would make the next append fuse unseen."""
    root = _lake(tmp_path / "lake")
    _append(root, _trim(SPY, "a" * 64))
    with trimmed_path(root).open("ab") as handle:
        handle.write(b'{"kind": "tr')
    entry_before = latest_entries(root)[TRIMMED_FILE]

    with lake_lock(root), pytest.raises(TrimmedTornTail):
        refresh_trimmed_entry(root, source=SOURCE, fetched_at=STAMP)

    assert latest_entries(root)[TRIMMED_FILE] == entry_before


def test_a_refresh_refuses_a_garbled_last_line_that_ends_in_a_newline(tmp_path):
    """Mutation this catches: the refresh checking only for the trailing newline.

    Certifying the line would let the Sunday scrub pass over a garbage line, since the reader
    drops it without a word and the entry's sha would match the bytes.
    """
    root = _lake(tmp_path / "lake")
    _append(root, _trim(SPY, "a" * 64))
    with trimmed_path(root).open("ab") as handle:
        handle.write(b'{"kind": "trim", "partition": "chains/ti\n')
    entry_before = latest_entries(root)[TRIMMED_FILE]

    with lake_lock(root), pytest.raises(TrimmedTornTail):
        refresh_trimmed_entry(root, source=SOURCE, fetched_at=STAMP)

    assert latest_entries(root)[TRIMMED_FILE] == entry_before


def test_a_refresh_records_the_bytes_on_disk(tmp_path):
    root = _lake(tmp_path / "lake")
    _append(root, _trim(SPY, "a" * 64))
    with trimmed_path(root).open("ab") as handle:
        handle.write((json.dumps(_trim(SPY_NEXT, "b" * 64), sort_keys=True) + "\n").encode())

    with lake_lock(root):
        entry = refresh_trimmed_entry(root, source=SOURCE, fetched_at=STAMP)

    assert entry["rows"] == 2
    assert entry["sha256"] == sha256_file(trimmed_path(root))


# -- the reader's refusals name this ledger ------------------------------------


def test_a_tear_in_the_body_refuses_in_this_ledgers_words(tmp_path):
    """A refusal's text becomes a Sunday problem line word for word, so it must not say verdict."""
    root = _lake(tmp_path / "lake")
    path = trimmed_path(root)
    good = json.dumps(_trim(SPY, "a" * 64), sort_keys=True)
    path.write_text(f'{good}\n{{"kind": "tr\n{good}\n')

    with pytest.raises(TornLedger) as refused:
        read_trimmed(root)

    message = str(refused.value)
    assert "trimmed on purpose" in message
    assert "verdict" not in message and "withholds" not in message


@pytest.mark.parametrize(
    ("raw", "refusal"),
    [(b"\xff\n", LedgerNotUtf8), (b"\xef\xbb\xbf{}\n", LedgerHasByteOrderMark)],
)
def test_undecodable_or_marked_bytes_refuse_in_this_ledgers_words(tmp_path, raw, refusal):
    root = _lake(tmp_path / "lake")
    trimmed_path(root).write_bytes(raw)

    with pytest.raises(refusal) as refused:
        read_trimmed(root)

    message = str(refused.value)
    assert "trimmed on purpose" in message
    assert "verdict" not in message and "withholds" not in message


def test_a_torn_trailing_line_is_discarded_as_the_partition_still_present(tmp_path):
    root = _lake(tmp_path / "lake")
    line = _trim(SPY, "a" * 64)
    trimmed_path(root).write_text(json.dumps(line, sort_keys=True) + '\n{"kind": "tr')

    assert read_trimmed(root) == [line]


# -- the Sunday scrub ----------------------------------------------------------


def test_with_no_ledger_the_scrub_answers_as_it_did_before(tmp_path):
    root = _lake(tmp_path / "lake")
    (root / SPY).unlink()

    result = scrub(root)

    assert result == ScrubResult(missing=(SPY,), sha_mismatches=(), orphans=())
    assert result.trimmed_unreadable is None
    assert not trimmed_path(root).exists()


def test_a_designed_absence_is_not_missing_and_the_scrub_passes(tmp_path):
    root = _lake(tmp_path / "lake")
    _trim_away(root, SPY)

    result = scrub(root)

    assert result.missing == ()
    assert result.ok, result


def test_a_partition_missing_beside_a_ledger_that_does_not_name_it_is_missing(tmp_path):
    root = _lake(tmp_path / "lake")
    _trim_away(root, SPY)
    (root / SPY_NEXT).unlink()

    result = scrub(root)

    assert result.missing == (SPY_NEXT,)
    assert not result.ok


@pytest.mark.parametrize(
    "line",
    [
        pytest.param(lambda sha: restore_line(SPY, sha256=sha, restored_at=STAMP), id="restored"),
        pytest.param(lambda sha: _trim(SPY, "f" * 64), id="a different sha"),
    ],
)
def test_a_ledger_mention_that_is_not_a_designed_absence_stays_missing(tmp_path, line):
    """Mutation this catches: treating any ledger mention of a partition as designed."""
    root = _lake(tmp_path / "lake")
    sha = latest_entries(root)[SPY]["sha256"]
    _append(root, _trim(SPY, sha))
    _append(root, line(sha))
    (root / SPY).unlink()

    assert scrub(root).missing == (SPY,)


def test_a_torn_ledger_fails_closed_and_names_itself(tmp_path):
    """Mutation this catches: skipping the fail-closed branch.

    The ledger's first line is a real trim line for the absent partition. A tear after it hides
    the line behind, so the read refuses. A scrub that read the lines in front of the tear and
    used them would call the partition designed and pass, which is the fail-open this rules out.
    """
    root = _lake(tmp_path / "lake")
    _trim_away(root, SPY)
    (root / SPY_NEXT).unlink()
    path = trimmed_path(root)
    path.write_bytes(path.read_bytes() + b'{"kind": "tr\n' + path.read_bytes())

    result = scrub(root)

    assert result.missing == (SPY, SPY_NEXT)
    assert result.trimmed_unreadable is not None
    assert result.trimmed_unreadable.startswith("TornLedger: ")
    assert "trimmed on purpose" in result.trimmed_unreadable
    assert not result.ok


def test_an_unreadable_ledger_alone_fails_the_scrub(tmp_path):
    """The field folds into ``ok`` even when every file is present."""
    root = _lake(tmp_path / "lake")
    trimmed_path(root).write_bytes(b"\xff\n")
    # Recorded, so the ledger is neither an orphan nor a sha mismatch and only the new field
    # can fail the scrub.
    record_partition(root, TRIMMED_FILE, source=SOURCE, rows=1, fetched_at=STAMP)

    result = scrub(root)

    assert (result.missing, result.sha_mismatches, result.orphans) == ((), (), ())
    assert result.trimmed_unreadable is not None
    assert not result.ok


def test_an_unreadable_ledger_fails_closed_rather_than_raise(tmp_path):
    """Mutation this catches: catching ``ManifestError`` alone, so an ``OSError`` raises.

    A directory at the ledger's path is a read that fails with ``IsADirectoryError``, the
    unreadable half of "torn or unreadable". It needs no ``chmod``, which a root runner ignores.
    The directory carries no manifest entry, so the forward pass never hashes it.
    """
    root = _lake(tmp_path / "lake")
    (root / SPY).unlink()
    trimmed_path(root).mkdir()

    result = scrub(root)

    assert result.missing == (SPY,)
    assert result.trimmed_unreadable is not None
    assert result.trimmed_unreadable.startswith("IsADirectoryError: ")
    assert not result.ok


# -- the repair of the ledger's manifest entry (marketlake #784) ---------------


def _repair(root: Path) -> bool:
    with lake_lock(root):
        return repair_trimmed_entry(root, source=SOURCE, fetched_at=STAMP)


def _write_line(root: Path, line: dict) -> None:
    """Append a whole line by hand, with no entry refresh, as a crash before the record leaves."""
    with trimmed_path(root).open("ab") as handle:
        handle.write((json.dumps(line, sort_keys=True) + "\n").encode())


def test_a_ledger_whose_entry_lags_an_appended_line_is_re_recorded(tmp_path):
    root = _lake(tmp_path / "lake")
    _append(root, _trim(SPY, "a" * 64))
    _write_line(root, restore_line(SPY, sha256="a" * 64, restored_at=STAMP))

    assert _repair(root) is True

    entry = latest_entries(root)[TRIMMED_FILE]
    assert entry["sha256"] == sha256_file(trimmed_path(root))
    assert entry["rows"] == 2


def test_a_ledger_with_no_entry_at_all_is_re_recorded(tmp_path):
    root = _lake(tmp_path / "lake")
    _write_line(root, _trim(SPY, "a" * 64))
    assert TRIMMED_FILE not in latest_entries(root)

    assert _repair(root) is True

    entry = latest_entries(root)[TRIMMED_FILE]
    assert entry["sha256"] == sha256_file(trimmed_path(root))
    assert entry["rows"] == 1


def test_a_ledger_in_step_with_its_entry_is_left_alone(tmp_path):
    """Mutation this catches: dropping the sha comparison, so every call re-records."""
    root = _lake(tmp_path / "lake")
    _append(root, _trim(SPY, "a" * 64))
    manifest = (root / "manifest.jsonl").read_bytes()

    assert _repair(root) is False

    assert (root / "manifest.jsonl").read_bytes() == manifest


def test_a_torn_tail_is_refused_and_never_re_recorded(tmp_path):
    """Mutation this catches: re-recording a torn tail, which certifies the fragment."""
    root = _lake(tmp_path / "lake")
    _append(root, _trim(SPY, "a" * 64))
    with trimmed_path(root).open("ab") as handle:
        handle.write(b'{"kind": "res')
    manifest = (root / "manifest.jsonl").read_bytes()

    with pytest.raises(TrimmedRepairRefused, match="Repair by hand under the lock"):
        _repair(root)

    assert (root / "manifest.jsonl").read_bytes() == manifest


def test_with_no_ledger_the_repair_reads_nothing_and_writes_nothing(tmp_path):
    """A host that never trims stays byte-identical and fails exactly as it did.

    The manifest here is damaged, so a repair that read it would raise. Mutation this catches:
    reading the manifest before checking the ledger exists.
    """
    root = _lake(tmp_path / "lake")
    with (root / "manifest.jsonl").open("ab") as handle:
        handle.write(b"\xff damaged\n")
    manifest = (root / "manifest.jsonl").read_bytes()

    assert _repair(root) is False

    assert (root / "manifest.jsonl").read_bytes() == manifest
    assert not trimmed_path(root).exists()


_LINE = json.dumps({"kind": "trim", "partition": SPY}).encode()


@pytest.mark.parametrize(
    ("raw", "cause"),
    [
        (b'{"partition": "\xff"}\n' + _LINE + b"\n", "LedgerNotUtf8"),
        (b"\xef\xbb\xbf" + _LINE + b"\n", "LedgerHasByteOrderMark"),
        (b"{\n" + _LINE + b"\n", "TornLedger"),
    ],
)
def test_an_unreadable_ledger_refuses_naming_its_cause_and_the_hand_repair(tmp_path, raw, cause):
    root = _lake(tmp_path / "lake")
    trimmed_path(root).write_bytes(raw)
    manifest = (root / "manifest.jsonl").read_bytes()

    with pytest.raises(TrimmedRepairRefused) as exc:
        _repair(root)

    assert cause in str(exc.value)
    assert "Repair by hand under the lock" in str(exc.value)
    assert (root / "manifest.jsonl").read_bytes() == manifest


def test_a_ledger_that_lost_lines_refuses_through_the_row_count_guard(tmp_path):
    """``record_partition``'s guard raises ``RowCountRegression``, which is no ``ManifestError``."""
    root = _lake(tmp_path / "lake")
    _append(root, _trim(SPY, "a" * 64))
    _append(root, _trim(SPY_NEXT, "b" * 64))
    first_line = trimmed_path(root).read_bytes().splitlines(keepends=True)[0]
    trimmed_path(root).write_bytes(first_line)

    with pytest.raises(TrimmedRepairRefused, match="lines were lost"):
        _repair(root)

    assert latest_entries(root)[TRIMMED_FILE]["rows"] == 2


def test_a_ledger_path_that_cannot_be_read_refuses_rather_than_raise(tmp_path):
    root = _lake(tmp_path / "lake")
    trimmed_path(root).mkdir()

    with pytest.raises(TrimmedRepairRefused, match="IsADirectoryError"):
        _repair(root)


def test_a_ledger_edited_in_place_is_refused_and_the_scrub_still_reports_it(tmp_path):
    """One character changed, the line count the same. Re-recording it would bless the edit.

    Mutation this catches: re-recording on any sha disagreement whose tail parses, which quiets
    the Sunday scrub's sha check on the ledger, and compaction would do it every night.
    """
    root = _lake(tmp_path / "lake")
    _append(root, _trim(SPY, "a" * 64))
    _append(root, _trim(SPY_NEXT, "b" * 64))
    path = trimmed_path(root)
    path.write_bytes(path.read_bytes().replace(b'"version_id": "v1"', b'"version_id": "v2"', 1))
    manifest = (root / "manifest.jsonl").read_bytes()

    with pytest.raises(TrimmedRepairRefused, match="edited in place or the bytes rotted"):
        _repair(root)

    assert (root / "manifest.jsonl").read_bytes() == manifest
    assert TRIMMED_FILE in scrub(root).sha_mismatches


def test_an_edit_in_place_behind_an_appended_line_is_still_refused(tmp_path):
    """A line landed after the entry does not excuse an edit to the lines the entry covers."""
    root = _lake(tmp_path / "lake")
    _append(root, _trim(SPY, "a" * 64))
    path = trimmed_path(root)
    path.write_bytes(path.read_bytes().replace(b'"version_id": "v1"', b'"version_id": "v2"', 1))
    _write_line(root, restore_line(SPY, sha256="a" * 64, restored_at=STAMP))

    with pytest.raises(TrimmedRepairRefused, match="edited in place"):
        _repair(root)


def test_a_damaged_manifest_is_named_as_the_manifest_and_not_the_ledger(tmp_path):
    """The repair for a damaged manifest is not the ledger's, so the text names manifest.jsonl."""
    root = _lake(tmp_path / "lake")
    _write_line(root, _trim(SPY, "a" * 64))
    manifest = root / "manifest.jsonl"
    manifest.write_bytes(manifest.read_bytes() + b'{"partition": "\xff"}\n')

    with pytest.raises(TrimmedRepairRefused) as exc:
        _repair(root)

    text = str(exc.value)
    assert text.startswith(f"{manifest}: the manifest could not be read (LedgerNotUtf8")
    assert "Repair manifest.jsonl by hand" in text
    assert "last line whole" not in text


@pytest.mark.parametrize(
    "between",
    [b"\n", " \n".encode(), b"\n\n"],
    ids=["blank-line", "no-break-space-line", "two-blank-lines"],
)
def test_an_entry_recorded_over_trailing_blank_lines_still_re_records_an_append(tmp_path, between):
    """The reader skips a line holding only whitespace, Unicode whitespace included.

    No writer leaves one, and a hand edit can. Compaction runs this repair every night, so
    counting such a line differently from the reader would refuse, and page, every night.
    Mutation this catches: counting lines by any rule but ``parse_jsonl``'s, or accepting only
    the offset right after the last counted line.
    """
    root = _lake(tmp_path / "lake")
    _append(root, _trim(SPY, "a" * 64))
    with trimmed_path(root).open("ab") as handle:
        handle.write(between)
    # Recorded by hand, as a person repairing the ledger would. The strict append's tail check
    # reads a last line of only U+00A0 as garbled, so it would refuse to record over one.
    record_partition(root, TRIMMED_FILE, source=SOURCE, rows=1, fetched_at=STAMP)
    _write_line(root, restore_line(SPY, sha256="a" * 64, restored_at=STAMP))

    assert _repair(root) is True

    entry = latest_entries(root)[TRIMMED_FILE]
    assert entry["rows"] == 2 and entry["sha256"] == sha256_file(trimmed_path(root))


def test_a_blank_line_inside_the_recorded_lines_still_re_records_an_append(tmp_path):
    root = _lake(tmp_path / "lake")
    line = (json.dumps(_trim(SPY, "a" * 64), sort_keys=True) + "\n").encode()
    trimmed_path(root).write_bytes(line + " \n".encode() + line)
    with lake_lock(root):
        refresh_trimmed_entry(root, source=SOURCE, fetched_at=STAMP)
    assert latest_entries(root)[TRIMMED_FILE]["rows"] == 2
    _write_line(root, restore_line(SPY, sha256="a" * 64, restored_at=STAMP))

    assert _repair(root) is True


def test_an_empty_ledger_recorded_at_no_rows_re_records_a_later_append(tmp_path):
    """A first append whose write landed nothing leaves an empty ledger, which the repair records
    at zero rows. A later append that crashes before its record is still a lagging append.

    Mutation this catches: dropping the zero-row case, which reads it as lost lines.
    """
    root = _lake(tmp_path / "lake")
    trimmed_path(root).write_bytes(b"")
    assert _repair(root) is True
    assert latest_entries(root)[TRIMMED_FILE]["rows"] == 0
    _write_line(root, _trim(SPY, "a" * 64))

    assert _repair(root) is True

    assert latest_entries(root)[TRIMMED_FILE]["rows"] == 1


def test_an_undecodable_line_after_the_entry_refuses_in_the_readers_words(tmp_path):
    """Mutation this catches: decoding before the reader runs, which names a bare
    ``UnicodeDecodeError`` rather than the ledger refusal the Sunday scrub also prints.
    """
    root = _lake(tmp_path / "lake")
    _append(root, _trim(SPY, "a" * 64))
    with trimmed_path(root).open("ab") as handle:
        handle.write(b'{"partition": "\xff"}\n')
        handle.write((json.dumps(_trim(SPY_NEXT, "b" * 64), sort_keys=True) + "\n").encode())

    with pytest.raises(TrimmedRepairRefused) as exc:
        _repair(root)

    assert "LedgerNotUtf8" in str(exc.value)
    assert "UnicodeDecodeError" not in str(exc.value)


# -- ported from the mutation lens on PR #810 ---------------------------------------


def test_a_torn_tail_refusal_carries_the_torn_tail_repair_once(tmp_path):
    """The torn tail names its own repair, so the generic hand repair is not appended to it."""
    root = _lake(tmp_path / "lake")
    _append(root, _trim(SPY, "a" * 64))
    with trimmed_path(root).open("ab") as handle:
        handle.write(b'{"kind": "res')

    with pytest.raises(TrimmedRepairRefused) as exc:
        _repair(root)

    assert str(exc.value).count("Repair by hand under the lock") == 1
