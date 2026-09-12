"""Manifest logic decided from values alone.

These cover the line parser, last-entry-wins, the torn-tail discard, the segment to
compacted-partition mapping the supersession rule leans on, and the reverse-pass
exclusion predicate. None of them touch a real lake. The disk-backed behaviors live in
the component tier.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lake.manifest import (
    SCRUB_EXCLUSIONS,
    ManifestError,
    _compacted_partition_for_segment,
    _is_excluded,
    _latest_by_partition,
    _parse_jsonl,
    latest_entries,
    latest_quarantine,
    manifest_path,
    quarantine_path,
)


def _entry(partition: str, **extra) -> dict:
    base = {"partition": partition, "source": "capture", "sha256": "x", "rows": 1}
    base.update(extra)
    return base


# -- line parsing and the torn tail ------------------------------------------


def test_parse_reads_every_complete_line():
    text = "".join(json.dumps(_entry(p)) + "\n" for p in ("a", "b", "c"))
    parsed = _parse_jsonl(text)
    assert [e["partition"] for e in parsed] == ["a", "b", "c"]


def test_parse_skips_blank_lines():
    text = json.dumps(_entry("a")) + "\n\n" + json.dumps(_entry("b")) + "\n"
    assert [e["partition"] for e in _parse_jsonl(text)] == ["a", "b"]


def test_parse_discards_a_torn_trailing_line():
    good = json.dumps(_entry("a")) + "\n" + json.dumps(_entry("b")) + "\n"
    torn = good + '{"partition": "c", "sha256": "untermin'
    parsed = _parse_jsonl(torn)
    assert [e["partition"] for e in parsed] == ["a", "b"]


def test_parse_of_empty_text_is_empty():
    assert _parse_jsonl("") == []


# -- last entry wins ---------------------------------------------------------


def test_last_entry_wins_per_partition():
    entries = [
        _entry("p", rows=100, sha256="old"),
        _entry("q", rows=5),
        _entry("p", rows=405, sha256="new"),
    ]
    latest = _latest_by_partition(entries, Path("manifest.jsonl"))
    assert latest["p"]["rows"] == 405
    assert latest["p"]["sha256"] == "new"
    assert latest["q"]["rows"] == 5


def test_latest_by_partition_of_nothing_is_empty():
    assert _latest_by_partition([], Path("manifest.jsonl")) == {}


# -- the segment to compacted-partition mapping ------------------------------


def test_segment_maps_to_its_compacted_partition():
    seg = "journal/date=2026-08-24/surface=chains/ticker=SPY/seg-20260824T160000-4242.arrows"
    assert _compacted_partition_for_segment(seg) == "chains/ticker=SPY/date=2026-08-24.parquet"


def test_segment_mapping_handles_the_quotes_surface():
    seg = "journal/date=2026-11-28/surface=quotes/ticker=QQQ/seg-20261128T130000-9.arrows"
    assert _compacted_partition_for_segment(seg) == "quotes/ticker=QQQ/date=2026-11-28.parquet"


@pytest.mark.parametrize(
    "rel",
    [
        "chains/ticker=SPY/date=2026-08-24.parquet",  # already a compacted partition
        "reference/security_master.parquet",  # not a segment at all
        "journal/date=2026-08-24/surface=chains/ticker=SPY/seg.parquet",  # wrong suffix
        # A Parquet file named like a segment. The filename is matched on its suffix, so
        # carrying the segment prefix is not enough.
        "journal/date=2026-08-24/surface=chains/ticker=SPY/seg-1.parquet",
        "journal/date=2026-08-24/ticker=SPY/seg-1.arrows",  # too few path parts
    ],
)
def test_non_segment_paths_map_to_none(rel: str):
    assert _compacted_partition_for_segment(rel) is None


def test_a_ticker_carrying_a_dot_maps_like_any_other():
    # The parser slices each part at its key prefix and passes the value through. A
    # ticker with a dot in it, the shape a class-B share takes, is not a special case.
    seg = "journal/date=2026-08-24/surface=quotes/ticker=BRK.B/seg-20260824T160000-7.arrows"
    assert _compacted_partition_for_segment(seg) == "quotes/ticker=BRK.B/date=2026-08-24.parquet"


@pytest.mark.parametrize(
    "rel",
    [
        # A segment path carries three keys under the journal directory, and the parser
        # matches each one as a literal prefix.
        #
        # 1. The date key.
        # 2. The surface key.
        # 3. The ticker key.
        #
        # Each key gets two cases. The first drops it. The second spells its separator
        # wrong, which is the harder one, because the key is still the right length. A
        # parser that sliced by length rather than matching the prefix would map such a
        # path onto a real-looking partition instead of refusing it. The journal
        # directory name is checked as well, so a path outside the journal gets a case.
        "journal/2026-08-24/surface=chains/ticker=SPY/seg-1.arrows",
        "journal/date:2026-08-24/surface=chains/ticker=SPY/seg-1.arrows",
        "journal/date=2026-08-24/chains/ticker=SPY/seg-1.arrows",
        "journal/date=2026-08-24/surface:chains/ticker=SPY/seg-1.arrows",
        "journal/date=2026-08-24/surface=chains/SPY/seg-1.arrows",
        "journal/date=2026-08-24/surface=chains/ticker:SPY/seg-1.arrows",
        "reports/date=2026-08-24/surface=chains/ticker=SPY/seg-1.arrows",
        # The part count is exact, so a deeper path is refused as well as a shallower one.
        "journal/date=2026-08-24/surface=chains/ticker=SPY/extra/seg-1.arrows",
    ],
)
def test_a_path_that_fails_one_of_the_parser_checks_maps_to_none(rel: str):
    assert _compacted_partition_for_segment(rel) is None


def test_supersession_decision_is_read_from_the_latest_dict():
    seg = "journal/date=2026-08-24/surface=chains/ticker=SPY/seg-20260824T160000-4242.arrows"
    compacted = _compacted_partition_for_segment(seg)
    # Without the compacted entry the segment stands on its own.
    assert compacted not in _latest_by_partition([_entry(seg)], Path("m.jsonl"))
    # With it present the segment is superseded.
    latest = _latest_by_partition([_entry(seg), _entry(compacted)], Path("m.jsonl"))
    assert compacted in latest


# -- the reverse-pass exclusion predicate ------------------------------------


def test_manifest_and_journal_are_excluded():
    assert _is_excluded("manifest.jsonl", SCRUB_EXCLUSIONS)
    assert _is_excluded(
        "journal/date=2026-08-24/surface=chains/ticker=SPY/seg-1.arrows",
        SCRUB_EXCLUSIONS,
    )


def test_data_files_and_the_quarantine_ledger_are_not_excluded():
    assert not _is_excluded("chains/ticker=SPY/date=2026-08-24.parquet", SCRUB_EXCLUSIONS)
    # The quarantine ledger carries its own manifest entry, so it is scrubbed, not skipped.
    assert not _is_excluded("quarantine.jsonl", SCRUB_EXCLUSIONS)


def test_enumerated_exclusion_set_is_exactly_the_three_documented_members():
    assert SCRUB_EXCLUSIONS == ("manifest.jsonl", "journal/", "reports/")


# -- a ledger line nobody can interpret ------------------------------------------------


def test_a_line_naming_no_partition_raises_and_locates_itself(tmp_path):
    """The integrity root refuses to be read past, and says which line to look at.

    Skipping the line was considered and rejected. A torn trailing line is a write that
    did not finish, which ``_read_jsonl`` already discards. A line in the body that parses
    and names nothing is a record no reader can interpret, and stepping over damage in the
    file every other check is measured against would make all of them weaker than they
    read.
    """
    manifest_path(tmp_path).write_text(
        '{"partition": "a.parquet", "rows": 1}\n{"source": "compaction", "rows": 406}\n'
    )

    with pytest.raises(ManifestError) as raised:
        latest_entries(tmp_path)

    assert "entry 2" in str(raised.value), "the error did not locate the bad line"
    assert str(manifest_path(tmp_path)) in str(raised.value), "the error did not name the ledger"


def test_a_line_that_is_not_an_object_raises_the_same_way(tmp_path):
    """Damage that indexes differently is damage the same, so it answers the same."""
    manifest_path(tmp_path).write_text("[1, 2, 3]\n")

    with pytest.raises(ManifestError):
        latest_entries(tmp_path)


def test_the_quarantine_ledger_names_itself_rather_than_the_manifest(tmp_path):
    """Two ledgers share the reader, so the message has to say which one broke."""
    quarantine_path(tmp_path).write_text('{"verdict": "keep"}\n')

    with pytest.raises(ManifestError) as raised:
        latest_quarantine(tmp_path)

    # Compared against the path itself. An earlier version asserted the word
    # "quarantine" appeared anywhere in the message, which pytest satisfies for free:
    # tmp_path is derived from the test's own name. It passed with the manifest's path
    # substituted, which is the mutation it existed to catch.
    assert str(quarantine_path(tmp_path)) in str(raised.value), str(raised.value)
