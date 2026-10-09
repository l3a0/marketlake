"""The trimmed ledger's designed-absence predicate and line schema, decided from values alone.

Marketlake #782. A designed absence is a partition whose latest manifest sha equals the sha on
its latest trimmed line, where that latest line is a trim line. Every reader of the lake that
meets an absent file asks ``is_designed_absence``, so this file checks the predicate's table and
that it decides from data alone, with no file on disk to stat. It also checks
``parse_trimmed``, the parse of a ledger's bytes that marketlake #785's restore reads the
bucket's copy with, and that ``read_trimmed`` goes through it.
"""

from __future__ import annotations

import inspect
import json
import os
from pathlib import Path

import pytest

from lake import trimmed as trimmed_module
from lake.manifest import LedgerHasByteOrderMark, LedgerNotUtf8, ManifestError, TornLedger
from lake.trimmed import (
    KIND_FIELD,
    RESTORE_KIND,
    TRIM_KIND,
    is_designed_absence,
    latest_by_partition,
    parse_trimmed,
    restore_line,
    trim_line,
)

PART = "chains/ticker=SPY/date=2026-09-01.parquet"
QUOTES = "quotes/ticker=SPY/date=2026-09-01.parquet"
OTHER = "chains/ticker=QQQ/date=2026-09-01.parquet"
SHA = "a" * 64
NEW_SHA = "b" * 64


def _trim(partition: str = PART, sha256: str = SHA) -> dict:
    return trim_line(
        partition,
        sha256=sha256,
        version_id="v1",
        verified_at="2026-10-07T16:40:00-04:00",
        trimmed_at="2026-10-07T16:41:00-04:00",
    )


def _restore(partition: str = PART, sha256: str = SHA) -> dict:
    return restore_line(partition, sha256=sha256, restored_at="2026-10-08T09:00:00-04:00")


def _manifest(sha256: str = SHA) -> dict[str, dict]:
    return {PART: {"partition": PART, "sha256": sha256, "rows": 10}}


# Each case is the ledger's lines in file order, the manifest's latest entries, and the answer.
# The lines go through ``latest_by_partition``, so a resolver that kept the first line per
# partition rather than the latest fails the two cases whose order decides.
CASES = [
    pytest.param([_trim()], _manifest(), True, id="a matching trim line is designed"),
    pytest.param(
        [_trim(), _restore()], _manifest(), False, id="a later restore line is not designed"
    ),
    pytest.param(
        [_restore(), _trim()], _manifest(), True, id="a trim after a restore is designed again"
    ),
    pytest.param(
        [_trim()], _manifest(NEW_SHA), False, id="a trim line whose sha differs is not designed"
    ),
    pytest.param(
        [_trim(sha256=NEW_SHA), _trim()],
        _manifest(),
        True,
        id="the latest trim line's sha is the one compared",
    ),
    pytest.param([], _manifest(), False, id="no ledger line is not designed"),
    pytest.param(
        [_trim(OTHER)], _manifest(), False, id="a line naming another partition is not designed"
    ),
    pytest.param([_trim()], {}, False, id="no manifest entry is not designed"),
    pytest.param(
        [{**_trim(), KIND_FIELD: "hold"}],
        _manifest(),
        False,
        id="a kind that is not trim is not designed",
    ),
    pytest.param(
        [{key: value for key, value in _trim().items() if key != "sha256"}],
        {PART: {"partition": PART, "rows": 10}},
        False,
        id="a trim line and an entry that both carry no sha are not designed",
    ),
]


@pytest.mark.parametrize(("lines", "manifest", "designed"), CASES)
def test_the_designed_absence_table(lines, manifest, designed):
    # Mutations this catches: dropping the sha comparison (the differing-sha case), ignoring
    # restore lines (the later-restore case), and resolving the first line per partition rather
    # than the latest (the later-restore and trim-after-restore cases).
    assert is_designed_absence(PART, manifest, latest_by_partition(lines)) is designed


def test_the_predicate_assumes_no_surface():
    """Chains are trimmed first, and every dated surface may be trimmed at the same cutoff."""
    manifest = {QUOTES: {"partition": QUOTES, "sha256": SHA, "rows": 10}}
    latest = latest_by_partition([_trim(QUOTES)])
    assert is_designed_absence(QUOTES, manifest, latest) is True
    assert _trim(QUOTES)["partition"] == QUOTES


def test_the_predicate_takes_no_root_or_path():
    """A rebuild asks it about the bucket's copies of both ledgers, where no local file exists."""
    names = list(inspect.signature(is_designed_absence).parameters)
    assert names == ["partition", "manifest_latest", "trimmed_latest"]


def test_the_predicate_never_touches_the_disk(tmp_path, monkeypatch):
    """Called with no lake on disk, and with every way of statting a file recorded.

    Mutation this catches: a stat added inside the predicate, such as a ``Path.exists`` check on
    the partition. Callers ask only about a file they already found absent, so a stat here would
    be a second answer to a question the caller settled, and one the bucket rebuild cannot ask.

    Each patched call is recorded and passed through rather than raised, and the record is read
    only after the patches are undone. A raise inside the patch would reach pytest's own failure
    reporting, which stats files too, and end the run in an internal error rather than a failure.
    """
    monkeypatch.chdir(tmp_path)
    latest = latest_by_partition([_trim()])
    touched: list[str] = []

    def recording(owner, name):
        original = getattr(owner, name)

        def record(*args, **kwargs):
            touched.append(name)
            return original(*args, **kwargs)

        return record

    with monkeypatch.context() as patch:
        for name in ("exists", "is_file", "stat", "lstat", "open", "read_bytes"):
            patch.setattr(Path, name, recording(Path, name))
        patch.setattr(os, "stat", recording(os, "stat"))
        patch.setattr(os.path, "exists", recording(os.path, "exists"))
        patch.setattr(os.path, "isfile", recording(os.path, "isfile"))
        designed = is_designed_absence(PART, _manifest(), latest)
        lost = is_designed_absence(OTHER, _manifest(), latest)

    assert touched == []
    assert designed is True
    assert lost is False


def test_the_schema_has_a_trim_kind_and_a_restore_kind_and_no_hold():
    """The owner's decision 5 on marketlake #755: no hold or release kind."""
    trim = _trim()
    restore = _restore()
    assert trim == {
        "kind": TRIM_KIND,
        "partition": PART,
        "sha256": SHA,
        "version_id": "v1",
        "verified_at": "2026-10-07T16:40:00-04:00",
        "trimmed_at": "2026-10-07T16:41:00-04:00",
    }
    assert restore == {
        "kind": RESTORE_KIND,
        "partition": PART,
        "sha256": SHA,
        "restored_at": "2026-10-08T09:00:00-04:00",
    }
    assert (TRIM_KIND, RESTORE_KIND) == ("trim", "restore")


@pytest.mark.parametrize(
    ("line", "says"),
    [
        ({"kind": "trim", "sha256": SHA}, "line 2 names no partition"),
        (["not", "an", "object"], "line 2 names no partition"),
        ({"kind": "trim", "partition": ["a"]}, "line 2 has a partition that cannot be a key"),
    ],
)
def test_a_line_naming_no_usable_partition_refuses_by_position(line, says):
    with pytest.raises(ManifestError, match=says):
        latest_by_partition([_trim(), line])


# -- parsing a ledger's bytes --------------------------------------------------------
#
# Marketlake #785. The restore parses the bucket's copy of the ledger, which has no lake root, so
# ``parse_trimmed`` takes bytes. ``read_trimmed`` calls it, so the damage refusals stay one
# implementation.


def _ledger(*lines: dict) -> bytes:
    return "".join(json.dumps(line, sort_keys=True) + "\n" for line in lines).encode()


def test_parse_trimmed_reads_whole_lines_and_discards_a_torn_tail():
    raw = _ledger(_trim(), _restore()) + b'{"kind": "tr'

    assert parse_trimmed(raw, "trimmed.jsonl") == [_trim(), _restore()]


@pytest.mark.parametrize(
    ("raw", "refusal"),
    [
        pytest.param(
            _ledger(_trim()) + b'{"kind": "tr\n' + _ledger(_restore()),
            TornLedger,
            id="hidden-tear",
        ),
        pytest.param(b"\xff\n", LedgerNotUtf8, id="bad-utf8"),
        pytest.param(b"\xef\xbb\xbf" + _ledger(_trim()), LedgerHasByteOrderMark, id="bom"),
    ],
)
def test_parse_trimmed_refuses_damage_in_this_ledgers_words(raw, refusal):
    with pytest.raises(refusal) as refused:
        parse_trimmed(raw, "trimmed.jsonl")

    message = str(refused.value)
    assert "trimmed.jsonl" in message
    assert "trimmed on purpose" in message


def test_read_trimmed_parses_through_parse_trimmed(tmp_path, monkeypatch):
    """Equal answers would also pass for a copy of the parse, so the call itself is checked.

    Mutation this catches: ``read_trimmed`` keeping a parse of its own, which the restore's
    reading of the bucket's copy would then not share.
    """
    raw = _ledger(_trim())
    (tmp_path / "trimmed.jsonl").write_bytes(raw)
    calls: list[tuple[bytes, Path]] = []
    answer = [{"partition": "from the stand-in"}]

    def stand_in(data: bytes, path: Path | str) -> list[dict]:
        calls.append((data, Path(path)))
        return answer

    monkeypatch.setattr(trimmed_module, "parse_trimmed", stand_in)

    assert trimmed_module.read_trimmed(tmp_path) is answer
    assert calls == [(raw, tmp_path / "trimmed.jsonl")]


@pytest.mark.parametrize(
    ("raw", "refusal", "consequence"),
    [
        pytest.param(
            b"\xff\n",
            LedgerNotUtf8,
            "Every line in this file is unreadable until that byte is repaired",
            id="bad-utf8",
        ),
        pytest.param(
            b"\xef\xbb\xbf" + _ledger(_trim()),
            LedgerHasByteOrderMark,
            "Read past it, a line is either discarded or filed under a name no reader asks about",
            id="bom",
        ),
        pytest.param(
            _ledger(_trim()) + b'{"kind": "tr\n' + _ledger(_restore()),
            TornLedger,
            "Every line behind that line is invisible",
            id="hidden-tear",
        ),
    ],
)
def test_parse_trimmed_names_the_path_it_was_handed_and_the_damage_s_own_cost(
    raw, refusal, consequence
):
    """Mutations this catches: naming a fixed file rather than ``path``, and swapping two
    consequence sentences. Each sentence is written out here rather than imported."""
    with pytest.raises(refusal) as refused:
        parse_trimmed(raw, "/some/where/trimmed.jsonl")

    message = str(refused.value)
    assert "/some/where/trimmed.jsonl" in message
    assert consequence in message
