"""The journal metadata stamp: two writers, one file, and a total reader.

The stamp carries the three facts the Now panel cannot read off a captured row: the
refresh token's mint time, the roster, and the last dead-man ping. It decides nothing
and touches no clock, so the tier is unit.

Four properties are covered here, because each one is what a panel field rests on.

1. The two writers share the file without clobbering each other. The cycle stamps the
   mint time and the roster, the dead-man stamps its ping, and each carries the other's
   keys forward.
2. The roster is stored as the surfaces each ticker is captured on, so a ticker that
   journaled nothing still has rows to show as failing.
3. The mint stamp is a timestamp. No token material reaches the file.
4. Reading is total. Absent, corrupt, or naive-timestamped, the stamp reads as an empty
   record rather than raising into a panel.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from lake.metadata import (
    JournalMetadata,
    metadata_path,
    read_metadata,
    stamp_cycle,
    stamp_ping,
)
from lake.tickers import Roster

ET = ZoneInfo("America/New_York")
MINTED = datetime(2026, 8, 30, 20, 5, tzinfo=ET)  # a Sunday evening re-auth
SLOT = datetime(2026, 8, 31, 9, 30, tzinfo=ET)
PING = datetime(2026, 8, 31, 9, 31, tzinfo=ET)


def _roster() -> Roster:
    return Roster.from_mapping(
        {
            "SPY": {"options": True, "chain_cadence": "1m"},
            "XYZ": {"options": False},
        }
    )


def test_a_cycle_stamp_carries_the_mint_time_the_roster_and_its_own_instant(lake_root):
    stamp_cycle(lake_root, at=SLOT, token_minted_at=MINTED, roster=_roster())

    stamp = read_metadata(lake_root)
    assert stamp.stamped_at == SLOT
    assert stamp.token_minted_at == MINTED
    # An equity-only ticker is expected on quotes alone, the same rule capture plans by.
    assert stamp.tickers == {"SPY": ("chains", "quotes"), "XYZ": ("quotes",)}
    assert stamp.dead_man_last_ping is None


def test_the_stamp_lands_at_the_journal_root(lake_root):
    stamp_cycle(lake_root, at=SLOT, token_minted_at=MINTED, roster=_roster())

    path = metadata_path(lake_root)
    assert path == lake_root / "journal" / "metadata.json"
    # A file inside a ``date=`` directory would keep that shell alive past its seal.
    assert [child.name for child in (lake_root / "journal").iterdir()] == ["metadata.json"]


def test_the_two_writers_carry_each_others_keys_forward(lake_root):
    stamp_cycle(lake_root, at=SLOT, token_minted_at=MINTED, roster=_roster())
    stamp_ping(lake_root, at=PING)

    stamp = read_metadata(lake_root)
    assert stamp.dead_man_last_ping == PING
    assert stamp.token_minted_at == MINTED
    assert stamp.tickers == {"SPY": ("chains", "quotes"), "XYZ": ("quotes",)}

    # And the other way round: the next cycle's stamp keeps the ping.
    later = SLOT.replace(minute=32)
    stamp_cycle(lake_root, at=later, token_minted_at=MINTED, roster=_roster())
    again = read_metadata(lake_root)
    assert again.dead_man_last_ping == PING
    assert again.stamped_at == later


def test_a_ping_alone_writes_a_stamp_with_nothing_else_in_it(lake_root):
    stamp_ping(lake_root, at=PING)

    stamp = read_metadata(lake_root)
    assert stamp == JournalMetadata(dead_man_last_ping=PING)


def test_a_later_roster_replaces_the_earlier_one(lake_root):
    stamp_cycle(lake_root, at=SLOT, token_minted_at=MINTED, roster=_roster())
    stamp_cycle(
        lake_root,
        at=SLOT,
        token_minted_at=MINTED,
        roster=Roster.from_mapping({"QQQ": {"options": True, "chain_cadence": "1m"}}),
    )

    # A retired ticker leaves the stamp on the next cycle, so the panel stops expecting
    # it. Its captured days still reach the panel through the lake's own layout.
    assert read_metadata(lake_root).tickers == {"QQQ": ("chains", "quotes")}


def test_the_file_holds_the_mint_timestamp_and_no_token_material(lake_root):
    stamp_cycle(lake_root, at=SLOT, token_minted_at=MINTED, roster=_roster())

    payload = json.loads(metadata_path(lake_root).read_text())
    assert set(payload) == {"stamped_at", "token_minted_at", "tickers"}
    assert payload["token_minted_at"] == MINTED.isoformat()
    # The whole file, read as text, is the guard. A secret cannot hide in a key this
    # test does not name if no secret-shaped value is present at all.
    assert "token" not in json.dumps(payload["tickers"])


def test_an_absent_stamp_reads_as_an_empty_record(lake_root):
    assert read_metadata(lake_root) == JournalMetadata()
    assert read_metadata(lake_root).tickers == {}


def test_a_corrupt_stamp_reads_as_an_empty_record(lake_root):
    path = metadata_path(lake_root)
    path.parent.mkdir(parents=True)
    path.write_text("{not json at all")

    assert read_metadata(lake_root) == JournalMetadata()


def test_a_stamp_that_is_json_but_not_an_object_reads_as_empty(lake_root):
    path = metadata_path(lake_root)
    path.parent.mkdir(parents=True)
    path.write_text("[1, 2, 3]")

    assert read_metadata(lake_root) == JournalMetadata()


def test_a_naive_timestamp_is_refused_rather_than_assumed_local(lake_root):
    path = metadata_path(lake_root)
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"token_minted_at": "2026-08-30T20:05:00"}))

    # The panel subtracts this from its own aware instant. A guessed offset would report
    # an age hours wrong, which is worse than reporting nothing.
    assert read_metadata(lake_root).token_minted_at is None


def test_a_misshapen_ticker_map_reads_as_far_as_it_parses(lake_root):
    path = metadata_path(lake_root)
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"tickers": {"SPY": ["chains", 7], "QQQ": "quotes"}}))

    # The list's non-string entry is dropped and the ticker whose surfaces are not a
    # list is dropped whole. Neither raises into the panel.
    assert read_metadata(lake_root).tickers == {"SPY": ("chains",)}


def test_a_stamp_is_published_by_one_rename_and_leaves_no_temp_file(lake_root):
    stamp_cycle(lake_root, at=SLOT, token_minted_at=MINTED, roster=_roster())
    stamp_ping(lake_root, at=PING)

    assert not list(metadata_path(lake_root).parent.glob("*.tmp-*"))


def test_the_mint_time_keeps_its_own_offset_through_the_round_trip(lake_root):
    utc_mint = MINTED.astimezone(UTC)
    stamp_cycle(lake_root, at=SLOT, token_minted_at=utc_mint, roster=_roster())

    # The vendor hands back UTC and the file keeps it. The panel converts to Eastern on
    # the way out, so the stored instant is the same moment either way.
    assert json.loads(metadata_path(lake_root).read_text())["token_minted_at"].endswith("+00:00")
    assert read_metadata(lake_root).token_minted_at == MINTED


def test_the_publish_is_the_rename_and_nothing_writes_the_target_in_place(lake_root, monkeypatch):
    """The dashboard reads this file while the daemon rewrites it every minute.

    The module's own docstring makes the rename load-bearing: a reader meets the old
    stamp or the new one, never half of either. A copy into the target would satisfy
    every other test in this file while giving a reader a window onto a half-written
    document.

    Failing the rename is how that is checked. If the publish is the rename, the target
    still holds the old stamp afterwards. If anything writes the target directly, the
    new stamp is already there and this fails.
    """
    stamp_cycle(lake_root, at=SLOT, token_minted_at=MINTED, roster=_roster())
    before = metadata_path(lake_root).read_text()

    def refuse(src, dst):
        raise OSError("rename refused")

    monkeypatch.setattr("lake.metadata.os.replace", refuse)
    later = SLOT.replace(hour=SLOT.hour + 1)
    try:
        stamp_cycle(lake_root, at=later, token_minted_at=MINTED, roster=_roster())
    except OSError:
        pass

    assert metadata_path(lake_root).read_text() == before
    # The temp file is cleaned up on the way out, so a refused publish leaves no litter.
    assert [p.name for p in metadata_path(lake_root).parent.iterdir()] == [
        metadata_path(lake_root).name
    ]
