"""Manifest logic decided from values alone.

These cover the line parser, last-entry-wins, the torn-tail discard, the segment to
compacted-partition mapping the supersession rule leans on, and the reverse-pass
exclusion predicate. None of them touch a real lake. The disk-backed behaviors live in
the component tier.
"""

from __future__ import annotations

import json

import pytest

from lake.manifest import (
    SCRUB_EXCLUSIONS,
    _compacted_partition_for_segment,
    _is_excluded,
    _latest_by_partition,
    _parse_jsonl,
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
    latest = _latest_by_partition(entries)
    assert latest["p"]["rows"] == 405
    assert latest["p"]["sha256"] == "new"
    assert latest["q"]["rows"] == 5


def test_latest_by_partition_of_nothing_is_empty():
    assert _latest_by_partition([]) == {}


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
        "journal/date=2026-08-24/ticker=SPY/seg-1.arrows",  # too few path parts
    ],
)
def test_non_segment_paths_map_to_none(rel: str):
    assert _compacted_partition_for_segment(rel) is None


def test_supersession_decision_is_read_from_the_latest_dict():
    seg = "journal/date=2026-08-24/surface=chains/ticker=SPY/seg-20260824T160000-4242.arrows"
    compacted = _compacted_partition_for_segment(seg)
    # Without the compacted entry the segment stands on its own.
    assert compacted not in _latest_by_partition([_entry(seg)])
    # With it present the segment is superseded.
    latest = _latest_by_partition([_entry(seg), _entry(compacted)])
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


def test_enumerated_exclusion_set_is_exactly_the_two_documented_members():
    assert SCRUB_EXCLUSIONS == ("manifest.jsonl", "journal/")
