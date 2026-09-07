"""Which manifest entries are one ticker's chains segments, decided from values alone.

``latest_expirations`` walks the manifest newest first and opens only the entries that
name the ticker's chains journal segments. ``_is_chains_segment_for`` is the predicate
that picks them out. It asks ``lake.paths.parse_segment_rel`` whether the path has the
segment shape, then checks the two values this reader alone cares about, the surface and
the ticker.

These pass strings and read booleans. No file is opened and no clock is read, so the
tier is unit. The disk-backed read that uses the predicate sits in the component tier,
in ``tests/component/test_latest_expirations.py``.

The malformed cases mirror the ones pinned in ``tests/unit/test_paths.py``. That is
deliberate. Both this reader and the manifest scrub go through the one shared parser, so
a weakened check has to be refused on both sides or one caller silently accepts a path
the other refuses.
"""

from __future__ import annotations

import pytest

from lake.journal import _is_chains_segment_for

SPY_SEGMENT = "journal/date=2026-08-24/surface=chains/ticker=SPY/seg-20260824T160000-4242.arrows"


def test_a_chains_segment_for_the_ticker_matches():
    assert _is_chains_segment_for(SPY_SEGMENT, "SPY")


def test_the_match_is_per_ticker_and_per_surface():
    # The read is scoped to one ticker's chains. Another ticker's segment and another
    # surface's segment both sit in the same manifest, so neither may qualify.
    assert not _is_chains_segment_for(SPY_SEGMENT, "QQQ")
    quotes = "journal/date=2026-08-24/surface=quotes/ticker=SPY/seg-20260824T160000-4242.arrows"
    assert not _is_chains_segment_for(quotes, "SPY")


def test_a_ticker_carrying_a_dot_matches_like_any_other():
    # The parser slices each part at its key prefix and passes the value through. A
    # ticker with a dot in it, the shape a class-B share takes, is not a special case.
    rel = "journal/date=2026-08-24/surface=chains/ticker=BRK.B/seg-20260824T160000-7.arrows"
    assert _is_chains_segment_for(rel, "BRK.B")
    assert not _is_chains_segment_for(rel, "BRK")


@pytest.mark.parametrize(
    "rel",
    [
        # A compacted partition. The manifest holds these beside the segments.
        "chains/ticker=SPY/date=2026-08-24.parquet",
        # The part count is exact. Four parts is one short, six is one long.
        "journal/date=2026-08-24/ticker=SPY/seg-1.arrows",
        "journal/date=2026-08-24/surface=chains/ticker=SPY/extra/seg-1.arrows",
        # The first part is the journal directory and nothing else.
        "reports/date=2026-08-24/surface=chains/ticker=SPY/seg-1.arrows",
        # Each of the three keys is matched as a literal prefix. A missing key fails.
        "journal/2026-08-24/surface=chains/ticker=SPY/seg-1.arrows",
        "journal/date=2026-08-24/chains/ticker=SPY/seg-1.arrows",
        "journal/date=2026-08-24/surface=chains/SPY/seg-1.arrows",
        # A wrong separator fails too. Each of these keys is the right length, so a
        # parser that sliced by length rather than matching the prefix would hand this
        # reader "chains" and "SPY" and the entry would qualify.
        "journal/date:2026-08-24/surface=chains/ticker=SPY/seg-1.arrows",
        "journal/date=2026-08-24/surface:chains/ticker=SPY/seg-1.arrows",
        "journal/date=2026-08-24/surface=chains/ticker:SPY/seg-1.arrows",
        # The filename is matched on its suffix, not its prefix. A segment-named Parquet
        # file carries the prefix and is not a segment.
        "journal/date=2026-08-24/surface=chains/ticker=SPY/seg-1.parquet",
        # The surface and the ticker sit in a fixed order. This path names the surface
        # SPY and the ticker chains, so a parser that returned the two the other way
        # round would read it as SPY's chains segment.
        "journal/date=2026-08-24/surface=SPY/ticker=chains/seg-1.arrows",
    ],
)
def test_a_path_that_is_not_spy_s_chains_segment_never_matches(rel: str):
    assert not _is_chains_segment_for(rel, "SPY")
