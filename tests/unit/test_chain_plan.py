"""The chain chunk plan: a pure-value module, no file or clock crossed.

These tests build and validate plans in memory and read throwaway plan files under the
test's temp directory. No process, network, or wall clock is crossed, so they sit in the
unit tier. They pin the tiling invariant, the date arithmetic, and the fail-safe load.
"""

from __future__ import annotations

import json
from datetime import date

import pytest

from lake.chain_plan import (
    DEFAULT_CHAIN_PLAN,
    ChainPlan,
    ChainPlanError,
    load_chain_plan,
)

# -- the tiling invariant ----------------------------------------------------


def test_default_plan_tiles_from_zero_with_an_open_tail():
    windows = DEFAULT_CHAIN_PLAN.windows
    # The first window starts at offset 0 and the last is open-ended.
    assert windows[0][0] == 0
    assert windows[-1][1] is None
    # Every non-last window is closed and the next starts one day after it ends, so the
    # tiling has no gap and no overlap.
    for (start, end), (next_start, _next_end) in zip(windows[:-1], windows[1:], strict=True):
        assert end is not None
        assert end >= start
        assert next_start == end + 1


def test_construction_rejects_a_non_zero_first_start():
    with pytest.raises(ChainPlanError):
        ChainPlan(((1, 9), (10, None)))


def test_construction_rejects_a_gap_between_windows():
    # A gap: window 1 should start at 10 to abut window 0's last day (9), not 11.
    with pytest.raises(ChainPlanError):
        ChainPlan(((0, 9), (11, None)))


def test_construction_rejects_an_overlap_between_windows():
    # An overlap: window 1 starts at 9, the same day window 0 ends on.
    with pytest.raises(ChainPlanError):
        ChainPlan(((0, 9), (9, None)))


def test_construction_rejects_a_closed_last_window():
    with pytest.raises(ChainPlanError):
        ChainPlan(((0, 9), (10, 30)))


def test_construction_rejects_a_mid_plan_open_window():
    with pytest.raises(ChainPlanError):
        ChainPlan(((0, None), (1, None)))


def test_construction_rejects_an_empty_plan():
    with pytest.raises(ChainPlanError):
        ChainPlan(())


def test_a_single_open_window_is_a_valid_plan():
    # The whole line covered by one open-ended window from offset 0.
    plan = ChainPlan(((0, None),))
    assert plan.windows_for(date(2026, 9, 1)) == [(date(2026, 9, 1), None)]


# -- date arithmetic ---------------------------------------------------------


def test_windows_for_adds_offsets_to_the_session_date():
    plan = ChainPlan(((0, 9), (10, 30), (31, None)))
    session = date(2026, 9, 1)
    assert plan.windows_for(session) == [
        (date(2026, 9, 1), date(2026, 9, 10)),
        (date(2026, 9, 11), date(2026, 10, 1)),
        (date(2026, 10, 2), None),
    ]


def test_windows_for_rolls_with_the_session_date():
    # The same plan against a later session date yields dates shifted by the same amount,
    # so a date-relative plan never goes stale.
    plan = ChainPlan(((0, 9), (10, None)))
    assert plan.windows_for(date(2027, 1, 15)) == [
        (date(2027, 1, 15), date(2027, 1, 24)),
        (date(2027, 1, 25), None),
    ]


# -- the fail-safe load ------------------------------------------------------


def _write(path, obj) -> None:
    path.write_text(json.dumps(obj))


def test_load_parses_a_valid_plan_file(tmp_path):
    path = tmp_path / "chain_plan.json"
    _write(
        path,
        {"windows": [{"start": 0, "end": 9}, {"start": 10, "end": 30}, {"start": 31, "end": None}]},
    )
    plan = load_chain_plan(path)
    assert plan.windows == ((0, 9), (10, 30), (31, None))


def test_load_returns_the_default_on_a_missing_file(tmp_path):
    assert load_chain_plan(tmp_path / "does-not-exist.json") is DEFAULT_CHAIN_PLAN


def test_load_returns_the_default_on_a_corrupt_file(tmp_path):
    path = tmp_path / "chain_plan.json"
    path.write_text("{ this is not valid json")
    assert load_chain_plan(path) is DEFAULT_CHAIN_PLAN


def test_load_returns_the_default_on_an_invalid_plan(tmp_path):
    # Well-formed JSON, but the windows do not tile from 0, so it fails validation and the
    # load falls back rather than raising on the hot path.
    path = tmp_path / "chain_plan.json"
    _write(path, {"windows": [{"start": 5, "end": 9}, {"start": 10, "end": None}]})
    assert load_chain_plan(path) is DEFAULT_CHAIN_PLAN


def test_load_returns_the_default_on_a_wrong_shape(tmp_path):
    # A JSON object missing the 'windows' key is not a plan.
    path = tmp_path / "chain_plan.json"
    _write(path, {"plan": []})
    assert load_chain_plan(path) is DEFAULT_CHAIN_PLAN
