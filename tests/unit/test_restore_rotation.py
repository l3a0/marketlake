"""Which files the weekly restore test reads, and which week a Sunday attempt counts as.

Both answers are decided from values alone. ``manifest.restore_picks`` takes the backup
scrub's matched pairs and a week number, and ``control_plane.restore_week`` takes the
attempt's UTC instant.

"Every file lands in some week" holds for any function into 0 to 51, a constant included,
so it would prove nothing here. Each residue below was computed once by hand, straight
from ``hashlib`` and outside the code under test, and is written as a literal. A change
to the hash, the byte slice, the byte order or the modulus moves at least one of them.
The week numbers are literal for the same reason.
"""

from __future__ import annotations

from datetime import UTC, datetime

from lake.control_plane import restore_week
from lake.manifest import restore_picks

# Each path with the residue sha256 gives it, computed by hand.
R0 = "chains/ticker=SPY/date=2026-03-18.parquet"  # residue 0
R2 = "chains/ticker=SPY/date=2026-05-03.parquet"  # residue 2
R7 = "chains/ticker=SPY/date=2026-02-05.parquet"  # residue 7
R51A = "chains/ticker=SPY/date=2026-03-17.parquet"  # residue 51
R51B = "chains/ticker=SPY/date=2026-05-07.parquet"  # residue 51

# The sha rides along unread, so any string serves.
PAIRS = [(R51B, "sha-51b"), (R7, "sha-7"), (R2, "sha-2"), (R51A, "sha-51a"), (R0, "sha-0")]


# -- restore_picks -------------------------------------------------------------------


def test_a_week_reads_the_files_whose_residue_it_is():
    assert restore_picks(PAIRS, 0) == ((R0, "sha-0"),)
    assert restore_picks(PAIRS, 2) == ((R2, "sha-2"),)
    assert restore_picks(PAIRS, 7) == ((R7, "sha-7"),)


def test_files_sharing_a_residue_are_read_together_in_path_order():
    # Handed over out of order, so the sort is the code's and not the fixture's.
    assert restore_picks(PAIRS, 51) == ((R51A, "sha-51a"), (R51B, "sha-51b"))
    # And the shas sort the other way round, so a sort on the sha would get it wrong.
    assert restore_picks([(R51B, "a"), (R51A, "z")], 51) == ((R51A, "z"), (R51B, "a"))


def test_the_week_number_wraps_every_52_weeks():
    assert restore_picks(PAIRS, 52) == ((R0, "sha-0"),)
    assert restore_picks(PAIRS, 59) == ((R7, "sha-7"),)
    assert restore_picks(PAIRS, 103) == ((R51A, "sha-51a"), (R51B, "sha-51b"))


def test_an_empty_week_takes_the_next_residue_that_holds_a_file():
    # Residues 3 to 6 hold nothing here, so week 3 reads residue 7's file.
    assert restore_picks(PAIRS, 3) == ((R7, "sha-7"),)
    assert restore_picks(PAIRS, 1) == ((R2, "sha-2"),)


def test_the_fallthrough_wraps_past_51_to_0():
    pairs = [(R2, "sha-2"), (R7, "sha-7"), (R0, "sha-0")]
    # Weeks 8 to 51 hold nothing, so week 8 goes round to residue 0.
    assert restore_picks(pairs, 8) == ((R0, "sha-0"),)
    # And week 52 is residue 0 itself, which is the first wrap of the rotation.
    assert restore_picks(pairs, 52) == ((R0, "sha-0"),)


def test_the_fallthrough_can_land_on_residue_51():
    # Weeks 8 to 50 hold nothing, so week 8 stops at 51 rather than going round to 2.
    assert restore_picks([(R2, "sha-2"), (R51A, "sha-51a")], 8) == ((R51A, "sha-51a"),)


def test_the_fallthrough_reaches_a_file_51_steps_on():
    # A lone file in residue 0, sought from week 1, is the longest walk there is.
    assert restore_picks([(R0, "sha-0")], 1) == ((R0, "sha-0"),)


def test_no_pairs_gives_no_picks():
    assert restore_picks([], 0) == ()
    assert restore_picks([], 39) == ()


def test_the_residue_reads_the_whole_path():
    # The two Sunday fixture partitions differ only in their surface directory, and they
    # land in different weeks.
    chains = "chains/ticker=SPY/date=2026-08-28.parquet"  # residue 7
    quotes = "quotes/ticker=SPY/date=2026-08-28.parquet"  # residue 2
    pairs = [(chains, "a"), (quotes, "b")]
    assert restore_picks(pairs, 7) == ((chains, "a"),)
    assert restore_picks(pairs, 2) == ((quotes, "b"),)
    # Week 34 finds nothing until it wraps round to residue 2.
    assert restore_picks(pairs, 34) == ((quotes, "b"),)


# -- restore_week --------------------------------------------------------------------

# Every instant is written in UTC, which is how the Sunday job's clock reads. The comment
# beside each says what it is in New York.


def test_the_epoch_sunday_is_week_0():
    assert restore_week(datetime(2026, 1, 5, 1, 0, tzinfo=UTC)) == 0  # Sun 2026-01-04 20:00 EST


def test_a_sunday_evening_counts_as_its_own_sunday_though_utc_reads_monday():
    assert restore_week(datetime(2026, 10, 5, 0, 0, tzinfo=UTC)) == 39  # Sun 20:00 EDT
    assert restore_week(datetime(2026, 10, 5, 3, 0, tzinfo=UTC)) == 39  # Sun 23:00 EDT


def test_a_monday_catch_up_counts_as_the_sunday_it_catches_up_for():
    assert restore_week(datetime(2026, 10, 5, 12, 25, tzinfo=UTC)) == 39  # Mon 08:25 EDT


def test_a_saturday_evening_belongs_to_the_week_before():
    # Already Sunday in UTC. Read in market time it is still Saturday, and that is the
    # case a UTC date would get wrong.
    assert restore_week(datetime(2026, 10, 4, 0, 0, tzinfo=UTC)) == 38  # Sat 20:00 EDT


def test_the_weekend_daylight_saving_ends():
    # 2026-11-01: clocks fall back at 02:00, so the evening is EST.
    assert restore_week(datetime(2026, 11, 2, 1, 0, tzinfo=UTC)) == 43  # Sun 20:00 EST
    assert restore_week(datetime(2026, 11, 2, 4, 0, tzinfo=UTC)) == 43  # Sun 23:00 EST
    assert restore_week(datetime(2026, 11, 2, 13, 25, tzinfo=UTC)) == 43  # Mon 08:25 EST


def test_a_later_sunday_at_23_00_est():
    assert restore_week(datetime(2026, 11, 9, 4, 0, tzinfo=UTC)) == 44  # Sun 2026-11-08 23:00 EST


def test_the_weekend_daylight_saving_starts():
    # 2027-03-14: clocks spring forward at 02:00, so the evening is EDT.
    assert restore_week(datetime(2027, 3, 15, 0, 0, tzinfo=UTC)) == 62  # Sun 20:00 EDT
    assert restore_week(datetime(2027, 3, 15, 3, 0, tzinfo=UTC)) == 62  # Sun 23:00 EDT
    assert restore_week(datetime(2027, 3, 15, 12, 25, tzinfo=UTC)) == 62  # Mon 08:25 EDT


def test_the_first_sunday_of_2027_wraps_to_residue_0():
    week = restore_week(datetime(2027, 1, 4, 1, 0, tzinfo=UTC))  # Sun 2027-01-03 20:00 EST
    assert week == 52
    assert restore_picks(PAIRS, week) == ((R0, "sha-0"),)
