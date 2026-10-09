"""The test that tells another host's manifest entries from a hand repair.

``lake.bucket.bucket_divergence`` reads the bucket's ``manifest.jsonl`` beside the lake's
when the copy is not a prefix. A bucket entry is foreign when its partition and sha256
pair appears in no entry anywhere in the lake's manifest. Marketlake #832 carries the
reasoning. Each decision it makes has a test here.

1. Every line is parsed on its own, on both sides, so a fused line hides nothing.
2. A line that is not an entry, or holds a byte that is not UTF-8, raises nothing.
3. A pair the lake recorded and later superseded is not foreign.
4. A rewrite of a recorded path under a new sha is foreign.
5. Only the latest entry for each partition in the bucket's tail counts.
6. The shared bytes stop at a line boundary, and their entry count parses each line.
"""

from __future__ import annotations

import hashlib
import json

import pytest

from lake.bucket import Divergence, bucket_divergence


def _line(partition: object, sha: object = None, source: str = "compaction") -> bytes:
    """One manifest entry, written the way ``manifest.append_entries`` writes it."""
    if sha is None:
        sha = hashlib.sha256(str(partition).encode()).hexdigest()
    entry = {"fetched_at": None, "partition": partition, "rows": 1, "sha256": sha, "source": source}
    return (json.dumps(entry, sort_keys=True) + "\n").encode()


def _fused(first: bytes, second: bytes) -> bytes:
    """A torn line with the next append concatenated onto it, as ``append_line`` leaves one."""
    return first[:-1] + second


def _foreign(split: Divergence) -> list[tuple[str, str]]:
    return [(entry["partition"], entry["sha256"]) for entry in split.foreign]


A = _line("chains/ticker=SPY/date=2026-10-01.parquet")
B = _line("quotes/ticker=SPY/date=2026-10-01.parquet")
C = _line("chains/ticker=SPY/date=2026-10-02.parquet")
D = _line("quotes/ticker=SPY/date=2026-10-02.parquet")
VM = "chains/ticker=SPY/date=2026-10-09.parquet"
LAPTOP = "bars/ticker=SPY/date=2026-10-09.parquet"


# -- 1. each line on its own --------------------------------------------------------


def test_a_foreign_entry_behind_a_fused_line_reads_as_foreign():
    lake = A + B + _line(LAPTOP)
    bucket = A + B + _fused(C, D) + _line(VM, "f" * 64)

    split = bucket_divergence(bucket, lake)

    assert _foreign(split) == [(VM, "f" * 64)]
    # The fused line names no entry, so the tail counts the one entry after it.
    assert split.bucket_tail == 1
    assert split.bucket_first == VM


def test_a_damaged_line_early_in_the_lake_still_leaves_its_later_pairs_recorded():
    # A hand repair dropped the lake's fused first line from the bucket's copy. Every entry
    # the copy keeps is still in the lake, behind the damage, so none is foreign.
    lake = _fused(A, B) + C + D
    bucket = C + D

    split = bucket_divergence(bucket, lake)

    assert split.foreign == ()
    assert split.shared_bytes == 0
    assert split.bucket_tail == 2


# -- 2. lines that are not entries -------------------------------------------------


def test_a_list_valued_partition_or_sha_raises_nothing():
    lake = A + _line("quotes/ticker=SPY/date=2026-10-01.parquet", ["not", "a", "sha"])
    # A line that parses to something other than an object is skipped too.
    bucket = (
        A + _line(["a", "list"]) + b'["an", "array"]\n7\n' + _line("chains/x.parquet", ["also"])
    )

    split = bucket_divergence(bucket, lake)

    # The list-valued partition is skipped. The list-valued sha reads as None, which the
    # lake never recorded for that path.
    assert _foreign(split) == [("chains/x.parquet", ["also"])]
    assert split.bucket_tail == 1


def test_a_line_holding_a_byte_that_is_not_utf8_raises_nothing_and_reads_as_foreign():
    lake = A + B
    rotted = B.replace(b'"sha256": "', b'"sha256": "\xff', 1)
    assert rotted != B
    bucket = A + rotted

    split = bucket_divergence(bucket, lake)

    assert [entry["partition"] for entry in split.foreign] == [json.loads(B)["partition"]]


def test_a_byte_that_is_not_utf8_on_the_lake_side_raises_nothing():
    lake = A + B.replace(b'"rows"', b'"\xfe"', 1) + C
    bucket = A + D

    split = bucket_divergence(bucket, lake)

    assert split.lake_tail == 2
    assert split.lake_first == json.loads(B)["partition"]


# -- 3. a superseded pair ----------------------------------------------------------


def test_a_pair_matching_a_superseded_lake_entry_is_not_foreign():
    # The lake recorded the bucket's pair and later superseded it, the way the 18:30 sweep
    # rewrites a path. The test is "anywhere in the lake", not "the lake's latest".
    path = "reference/capture_spans.parquet"
    lake = _line(path, "1" * 64) + A + _line(path, "2" * 64)
    bucket = A + _line(path, "1" * 64)

    split = bucket_divergence(bucket, lake)

    assert split.shared_bytes == 0
    assert split.bucket_tail == 2
    assert split.foreign == ()


# -- 4. a rewrite under a new sha ---------------------------------------------------


def test_a_tail_of_rewrites_of_existing_paths_with_new_shas_is_foreign():
    path_a = json.loads(A)["partition"]
    path_b = json.loads(B)["partition"]
    lake = A + B + C
    bucket = A + B + _line(path_a, "a" * 64) + _line(path_b, "b" * 64)

    split = bucket_divergence(bucket, lake)

    assert _foreign(split) == [(path_a, "a" * 64), (path_b, "b" * 64)]


# -- 5. latest within the bucket's tail ---------------------------------------------


def test_an_earlier_foreign_entry_its_own_host_superseded_is_not_counted():
    path = json.loads(C)["partition"]
    lake = A + C
    bucket = A + _line(path, "9" * 64) + C

    assert bucket_divergence(bucket, lake).foreign == ()


def test_a_later_foreign_entry_over_a_recorded_one_is_counted():
    path = json.loads(C)["partition"]
    lake = A + C
    bucket = A + C + _line(path, "9" * 64)

    assert _foreign(bucket_divergence(bucket, lake)) == [(path, "9" * 64)]


def test_the_foreign_entries_come_in_the_order_of_their_latest_entry():
    lake = A
    bucket = A + _line("x.parquet", "1" * 64) + _line("y.parquet", "2" * 64)
    bucket += _line("x.parquet", "3" * 64)

    split = bucket_divergence(bucket, lake)

    assert _foreign(split) == [("y.parquet", "2" * 64), ("x.parquet", "3" * 64)]
    assert split.bucket_tail == 3


# -- 6. the shared bytes and their count ---------------------------------------------


def test_a_first_difference_mid_line_cuts_back_to_that_lines_start():
    other = B.replace(b"2026-10-01", b"2026-10-31", 1)
    assert other != B and len(other) == len(B)

    split = bucket_divergence(A + other, A + B)

    assert (split.shared_bytes, split.shared) == (len(A), 1)
    assert (split.bucket_tail, split.lake_tail) == (1, 1)
    assert split.bucket_first == json.loads(other)["partition"]
    assert split.lake_first == json.loads(B)["partition"]


def test_a_first_difference_at_byte_zero_shares_nothing():
    split = bucket_divergence(b"X" + A[1:] + B, A + B)

    assert (split.shared_bytes, split.shared) == (0, 0)
    # The damaged line names no entry, so the bucket's tail counts the one after it.
    assert (split.bucket_tail, split.lake_tail) == (1, 2)


@pytest.mark.parametrize(
    ("bucket", "lake", "shared", "bucket_tail", "lake_tail"),
    [
        (A + B, A + B + C, 2, 0, 1),
        (A + B + C, A + B, 2, 1, 0),
        (A + B[:7], A + B, 1, 0, 1),
        (A + B, A + B, 2, 0, 0),
    ],
    ids=["bucket-shorter", "lake-shorter", "bucket-ends-mid-line", "equal"],
)
def test_a_fully_shared_shorter_side_ends_at_its_last_whole_line(
    bucket, lake, shared, bucket_tail, lake_tail
):
    split = bucket_divergence(bucket, lake)

    assert split.shared == shared
    assert split.shared_bytes == sum(len(line) for line in (A, B, C)[:shared])
    assert (split.bucket_tail, split.lake_tail) == (bucket_tail, lake_tail)
    # A whole entry past the lake's end is one the lake never recorded.
    assert len(split.foreign) == bucket_tail
    if bucket_tail == 0:
        assert split.bucket_first is None
    if lake_tail == 0:
        assert split.lake_first is None


def test_a_fused_line_inside_the_shared_bytes_does_not_stop_the_count():
    shared = A + _fused(B, C) + D
    split = bucket_divergence(shared + _line(VM, "f" * 64), shared + _line(LAPTOP))

    assert split.shared_bytes == len(shared)
    # parse_jsonl would stop at the fused line and count 1. Each line on its own counts 2.
    assert split.shared == 2
    assert _foreign(split) == [(VM, "f" * 64)]
    assert (split.bucket_first, split.lake_first) == (VM, LAPTOP)
