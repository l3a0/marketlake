"""The nightly window re-tune rule, decided from values alone.

``retune_plan`` takes a plan and each window's peak per-cycle contract count and rebuilds
the plan. These pin the rule: a window over the max splits at its midpoint offset, two
adjacent finite windows both under the min merge, the open tail is never split and never
merged, and the rebuilt plan still tiles. ``write_chain_plan`` crosses the filesystem, so
its atomicity check sits here with a temp directory rather than a lake. Nothing reads a
clock.
"""

from __future__ import annotations

import json
import os

import pytest

from lake.chain_plan import DEFAULT_CHAIN_PLAN, ChainPlan, load_chain_plan
from lake.compact import retune_plan, write_chain_plan
from lake.config import GuardConstants

GUARDS = GuardConstants()
MAX = GUARDS.chain_window_max_contracts
MIN = GUARDS.chain_window_min_contracts

# A steady profile: every window inside the band, so nothing splits or merges.
STEADY = {(0, 9): 2000, (10, 30): 1500, (31, 90): 1200, (91, 365): 1000, (366, None): 900}


def _retune(plan: ChainPlan, counts: dict) -> tuple[ChainPlan, tuple, tuple]:
    return retune_plan(plan, counts, max_contracts=MAX, min_contracts=MIN)


def test_the_pinned_constants_carry_the_measured_margin():
    # 2,500 contracts at about 1.2 KB each is about 3 MB, under the 7.4 MB response that
    # carried 6,278 contracts. 800 is under half of that, so a merged pair stays small.
    assert MAX == 2500
    assert MIN == 800
    assert 2 * MIN <= MAX


def test_a_steady_profile_leaves_the_plan_unchanged():
    after, splits, merges = _retune(DEFAULT_CHAIN_PLAN, STEADY)
    assert after == DEFAULT_CHAIN_PLAN
    assert splits == () and merges == ()


def test_a_window_over_the_max_splits_at_its_midpoint_offset():
    counts = {**STEADY, (10, 30): MAX + 1}
    after, splits, merges = _retune(DEFAULT_CHAIN_PLAN, counts)
    # (10, 30) splits at offset 20: (10, 20) and (21, 30). The rest are untouched.
    assert after.windows == ((0, 9), (10, 20), (21, 30), (31, 90), (91, 365), (366, None))
    assert splits == ((10, 30),)
    assert merges == ()


def test_a_window_exactly_at_the_max_does_not_split():
    counts = {**STEADY, (10, 30): MAX}
    after, splits, _ = _retune(DEFAULT_CHAIN_PLAN, counts)
    assert after == DEFAULT_CHAIN_PLAN
    assert splits == ()


def test_a_single_day_window_cannot_split():
    plan = ChainPlan(((0, 0), (1, None)))
    after, splits, _ = _retune(plan, {(0, 0): MAX * 3, (1, None): 100})
    assert after == plan
    assert splits == ()


def test_two_adjacent_small_finite_windows_merge():
    counts = {**STEADY, (10, 30): 100, (31, 90): 200}
    after, splits, merges = _retune(DEFAULT_CHAIN_PLAN, counts)
    assert after.windows == ((0, 9), (10, 90), (91, 365), (366, None))
    assert merges == (((10, 30), (31, 90)),)
    assert splits == ()


def test_a_merged_window_keeps_merging_while_its_sum_stays_under_the_min():
    counts = {(0, 9): 2000, (10, 30): 100, (31, 90): 100, (91, 365): 100, (366, None): 900}
    after, _, merges = _retune(DEFAULT_CHAIN_PLAN, counts)
    # 100 + 100 = 200 is still under the min, so the third small window joins too.
    assert after.windows == ((0, 9), (10, 365), (366, None))
    assert merges == (((10, 30), (31, 90)), ((10, 90), (91, 365)))


def test_a_merged_window_stops_merging_once_its_sum_reaches_the_min():
    half = MIN // 2
    counts = {(0, 9): 2000, (10, 30): half, (31, 90): half, (91, 365): half, (366, None): 900}
    after, _, merges = _retune(DEFAULT_CHAIN_PLAN, counts)
    # half + half reaches the min exactly, so the pair is no longer "under" and stops.
    assert after.windows == ((0, 9), (10, 90), (91, 365), (366, None))
    assert merges == (((10, 30), (31, 90)),)


def test_a_small_window_beside_a_large_one_does_not_merge():
    counts = {**STEADY, (10, 30): 100}
    after, _, merges = _retune(DEFAULT_CHAIN_PLAN, counts)
    assert after == DEFAULT_CHAIN_PLAN
    assert merges == ()


def test_the_open_tail_is_never_split_and_never_merged():
    counts = {**STEADY, (91, 365): 10, (366, None): MAX * 4}
    after, splits, merges = _retune(DEFAULT_CHAIN_PLAN, counts)
    # The tail is over the max and its neighbour is under the min. Neither trigger fires
    # on the tail, and it keeps its None end.
    assert after == DEFAULT_CHAIN_PLAN
    assert splits == () and merges == ()
    assert after.windows[-1] == (366, None)


def test_a_window_with_no_rows_counts_zero_and_merges():
    # A window absent from the profile had no contracts on any ticker. It is small.
    counts = {(0, 9): 2000, (10, 30): 100, (366, None): 900}
    after, _, merges = _retune(DEFAULT_CHAIN_PLAN, counts)
    assert after.windows == ((0, 9), (10, 365), (366, None))
    assert merges == (((10, 30), (31, 90)), ((10, 90), (91, 365)))


def test_an_unknown_window_neither_splits_nor_merges_nor_lets_a_merge_cross_it():
    # (31, 90) failed all day. Its neighbours are both small, but the merge cannot fold a
    # failing range into a healthy one, so nothing moves.
    counts = {(0, 9): 2000, (10, 30): 100, (31, 90): 0, (91, 365): 100, (366, None): 900}
    after, splits, merges = retune_plan(
        DEFAULT_CHAIN_PLAN,
        counts,
        max_contracts=MAX,
        min_contracts=MIN,
        unknown={(31, 90)},
    )
    assert after == DEFAULT_CHAIN_PLAN
    assert splits == () and merges == ()
    # Two small windows beside each other still merge when neither is unknown.
    after, _, merges = retune_plan(
        DEFAULT_CHAIN_PLAN,
        {**counts, (31, 90): 100},
        max_contracts=MAX,
        min_contracts=MIN,
        unknown={(0, 9)},
    )
    assert after.windows == ((0, 9), (10, 365), (366, None))
    assert merges == (((10, 30), (31, 90)), ((10, 90), (91, 365)))


def test_split_halves_never_merge_back():
    # A just-over-max window splits into halves each credited half the count, which the
    # pinned constants keep above the min. So the halves never merge with a small
    # neighbour on the same pass.
    counts = {(0, 9): MAX + 1, (10, 30): 10, (31, 90): 1500, (91, 365): 1000, (366, None): 900}
    after, splits, merges = _retune(DEFAULT_CHAIN_PLAN, counts)
    assert after.windows == ((0, 4), (5, 9), (10, 30), (31, 90), (91, 365), (366, None))
    assert splits == ((0, 9),)
    assert merges == ()


def test_the_rebuilt_plan_always_tiles():
    # Every combination of split and merge yields a ChainPlan, whose constructor validates
    # the tiling. Any gap or overlap would have raised inside retune_plan.
    counts = {(0, 9): MAX * 2, (10, 30): 10, (31, 90): 10, (91, 365): MAX * 2, (366, None): 5}
    after, _, _ = _retune(DEFAULT_CHAIN_PLAN, counts)
    assert after.windows == ((0, 4), (5, 9), (10, 90), (91, 228), (229, 365), (366, None))


# -- the plan file -----------------------------------------------------------


def test_write_chain_plan_round_trips_through_load_chain_plan(tmp_path):
    path = tmp_path / "config" / "chain_plan.json"
    plan = ChainPlan(((0, 4), (5, 9), (10, 90), (91, None)))
    write_chain_plan(plan, path)
    assert load_chain_plan(path) == plan
    # The file is the documented shape, with the open tail's end as JSON null.
    data = json.loads(path.read_text())
    assert data == {
        "windows": [
            {"start": 0, "end": 4},
            {"start": 5, "end": 9},
            {"start": 10, "end": 90},
            {"start": 91, "end": None},
        ]
    }
    assert not list(tmp_path.rglob("*.tmp-*"))


def test_write_chain_plan_is_atomic_on_a_failed_rename(tmp_path, monkeypatch):
    path = tmp_path / "chain_plan.json"
    original = ChainPlan(((0, 9), (10, None)))
    write_chain_plan(original, path)
    before = path.read_bytes()

    def refuse(src, dst):
        raise OSError("simulated rename failure")

    monkeypatch.setattr(os, "replace", refuse)
    with pytest.raises(OSError):
        write_chain_plan(ChainPlan(((0, 4), (5, None))), path)
    # The prior file is byte-identical and no partial file is left beside it.
    assert path.read_bytes() == before
    assert sorted(tmp_path.iterdir()) == [path]
    assert load_chain_plan(path) == original
