"""``read_token_mint`` decides from one field of one file, so it is a unit test.

The token file is ``schwab-py``'s shape: ``creation_timestamp`` beside a ``token``
object. Only the timestamp is read. Every failure is a ``ValueError`` the Sunday job
turns into a named problem, never a skip.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from lake.calendar import MARKET_TZ
from lake.control_plane import (
    SUNDAY_MAINTENANCE,
    TOKEN_LIFETIME,
    read_token_mint,
    sunday_canary_due,
)

MINTED = 1756596600  # 2026-08-30 23:30:00 UTC


def test_reads_creation_timestamp_as_an_aware_utc_datetime(tmp_path):
    path = tmp_path / "token.json"
    path.write_text(json.dumps({"creation_timestamp": MINTED, "token": {"access_token": "x"}}))
    mint = read_token_mint(path)
    assert mint == datetime.fromtimestamp(MINTED, tz=UTC)
    assert mint.tzinfo is not None


def test_a_missing_file_raises(tmp_path):
    with pytest.raises(ValueError, match="unreadable"):
        read_token_mint(tmp_path / "absent.json")


def test_a_file_that_is_not_json_raises(tmp_path):
    path = tmp_path / "token.json"
    path.write_text("not json")
    with pytest.raises(ValueError, match="not JSON"):
        read_token_mint(path)


def test_a_file_without_the_field_raises(tmp_path):
    path = tmp_path / "token.json"
    path.write_text(json.dumps({"token": {}}))
    with pytest.raises(ValueError, match="creation_timestamp"):
        read_token_mint(path)


def test_a_non_numeric_timestamp_raises(tmp_path):
    path = tmp_path / "token.json"
    path.write_text(json.dumps({"creation_timestamp": "soon"}))
    with pytest.raises(ValueError, match="epoch second"):
        read_token_mint(path)


def test_an_out_of_range_timestamp_raises(tmp_path):
    path = tmp_path / "token.json"
    path.write_text(json.dumps({"creation_timestamp": 1e20}))
    with pytest.raises(ValueError, match="epoch second"):
        read_token_mint(path)


def test_a_bool_or_numeric_string_is_refused(tmp_path):
    path = tmp_path / "token.json"
    for stamp in (True, "1756596600"):
        path.write_text(json.dumps({"creation_timestamp": stamp}))
        with pytest.raises(ValueError, match="epoch second"):
            read_token_mint(path)


# -- the Sunday the token in use is due to be replaced ------------------------

# The ritual repeats weekly at the 20:00 Eastern canary, so the deadline for one token
# is a function of its mint alone. The Now panel counts down to it.
SUNDAY = datetime(2026, 8, 30, 20, 30, tzinfo=MARKET_TZ)  # after that week's canary


@pytest.mark.parametrize(
    ("minted", "due"),
    [
        # A mint on any day but Sunday is due at the coming Sunday's canary.
        (datetime(2026, 9, 2, 11, 0, tzinfo=MARKET_TZ), datetime(2026, 9, 6, 20, 0)),
        (datetime(2026, 8, 31, 0, 1, tzinfo=MARKET_TZ), datetime(2026, 9, 6, 20, 0)),
        (datetime(2026, 9, 5, 23, 59, tzinfo=MARKET_TZ), datetime(2026, 9, 6, 20, 0)),
        # A Sunday mint has already cleared that Sunday's ritual, whether the ritual was
        # done before the canary or after it. Its own deadline is the following week.
        (SUNDAY, datetime(2026, 9, 6, 20, 0)),
        (datetime(2026, 8, 30, 19, 0, tzinfo=MARKET_TZ), datetime(2026, 9, 6, 20, 0)),
    ],
)
def test_the_due_canary_is_the_next_ritual_the_token_must_survive_to(minted, due):
    assert sunday_canary_due(minted) == due.replace(tzinfo=MARKET_TZ)


def test_the_deadline_is_read_in_eastern_time_whatever_zone_the_mint_carries():
    # The vendor hands back UTC. 2026-09-06 00:30 UTC is Saturday 20:30 Eastern, the
    # evening before the ritual, so the deadline is the next day's canary. Read in its
    # own zone it would be a Sunday, the ritual would look done, and the countdown would
    # point a whole week past the one the operator has to keep.
    minted = datetime(2026, 9, 6, 0, 30, tzinfo=UTC)
    assert sunday_canary_due(minted) == datetime(2026, 9, 6, 20, 0, tzinfo=MARKET_TZ)


def test_the_deadline_is_the_canary_moment_the_control_plane_pins():
    minted = datetime(2026, 9, 2, 11, 0, tzinfo=MARKET_TZ)
    due = sunday_canary_due(minted)
    assert (due.hour, due.minute) == (SUNDAY_MAINTENANCE.hour, SUNDAY_MAINTENANCE.minute)
    assert due.weekday() == 6  # Python numbers Sunday 6


def test_every_mint_of_a_week_gets_exactly_one_ritual_ahead_of_it():
    # Two properties hold for any mint. The deadline is ahead of it, so a fresh token
    # never counts down to a ritual already past. And it is at most one week and one day
    # out, so the countdown names the next ritual rather than skipping one.
    #
    # A Sunday mint in the small hours is the case worth naming. Its deadline sits hours
    # past the seven-day expiry. That costs nothing, because the span from expiry to
    # ritual is a closed market, which is why the design put the deadline there.
    for hours in range(0, 24 * 7, 5):
        minted = datetime(2026, 8, 30, 0, 0, tzinfo=MARKET_TZ) + timedelta(hours=hours)
        ahead = sunday_canary_due(minted) - minted
        assert timedelta(0) < ahead <= TOKEN_LIFETIME + timedelta(days=1)
