"""The corporate-actions ledger on disk.

Every test here builds a lake under a temp directory and reads the ledger back the way a
reader would, off the file. The security master is built in memory, because nothing in
this module fetches and the master's own round trip is covered beside it.

For a module whose job is a record format and a resolver, these tests are the
specification. Marketlake #282 names the eight behaviours they cover.
"""

from __future__ import annotations

import fcntl
import json
import os
import threading
import time
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from lake import actions
from lake.actions import (
    ACTIONS_PARTITION,
    PROVENANCE_MANUAL,
    PROVENANCE_OBSERVED,
    PROVENANCE_VENDOR_REPORTED,
    SWEEP_SOURCE,
    TYPE_DIVIDEND,
    TYPE_SPLIT,
    LedgerLineError,
    UnresolvedSymbol,
    actions_path,
)
from lake.lock import lake_lock
from lake.manifest import latest_entries, read_manifest, scrub
from lake.security_master import AmbiguousSymbol, SecurityMaster

# The observation day, and the two moments the ledger was written at. The correction lands
# seven weeks after the entry it supersedes, which is the look-ahead bias append-only
# exists to make visible rather than silent.
OBSERVED_ON = date(2026, 9, 14)
RECORDED = datetime(2026, 6, 19, 23, 0, tzinfo=UTC)  # 19:00 ET on 2026-06-19
CORRECTED = datetime(2026, 8, 5, 23, 0, tzinfo=UTC)  # 19:00 ET on 2026-08-05

SPY = 1
EX_DATE = "2026-06-18"


def _dividend(lake_root: Path, *, cash_amount: float, recorded_at: datetime, **overrides) -> dict:
    """Append one SPY dividend, taking the fields every test here shares."""
    fields = {
        "instrument_id": SPY,
        "observed_on": OBSERVED_ON,
        "recorded_at": recorded_at,
        "ex_date": EX_DATE,
        "type": TYPE_DIVIDEND,
        "pay_date": "2026-07-31",
        "declared_date": "2026-01-02",
        "cash_amount": cash_amount,
        "provenance": PROVENANCE_VENDOR_REPORTED,
    }
    fields.update(overrides)
    return actions.append(lake_root, **fields)


def _write_lines(lake_root: Path, entries: list[dict]) -> Path:
    """Write ledger lines by hand, for the damage ``append`` cannot produce."""
    path = actions_path(lake_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(entry) + "\n" for entry in entries))
    return path


def _master() -> SecurityMaster:
    """A master holding SPY, onboarded before the observation day."""
    master = SecurityMaster()
    master.register(
        kind="equity",
        capture_start=datetime(2026, 9, 8, 13, 30, tzinfo=UTC),
        valid_from=date(2026, 9, 8),
        ticker="SPY",
    )
    return master


# -- resolution in file order --------------------------------------------------


def test_two_entries_on_one_key_resolve_to_the_last_one_in_the_file(lake_root):
    # Resolution reads file order, never the highest ``recorded_at``. The correction is
    # appended second carrying the *earlier* stamp, which is what a clock stepped
    # backwards by an NTP correction or a restored machine would produce. The later entry
    # still wins, because rows are only ever appended so file order is monotonic.
    _dividend(lake_root, cash_amount=1.90352, recorded_at=CORRECTED)
    _dividend(lake_root, cash_amount=1.95, recorded_at=RECORDED)

    resolved = actions.latest(lake_root)
    assert list(resolved) == [(SPY, EX_DATE, TYPE_DIVIDEND)]
    assert resolved[(SPY, EX_DATE, TYPE_DIVIDEND)]["cash_amount"] == 1.95
    # The history is the record, so both entries are still readable.
    assert [entry["cash_amount"] for entry in actions.read(lake_root)] == [1.90352, 1.95]


def test_a_split_and_a_dividend_on_one_ex_date_are_two_keys(lake_root):
    # ``type`` is in the key because an instrument can pay and split on the same date.
    _dividend(lake_root, cash_amount=1.90352, recorded_at=RECORDED)
    actions.append(
        lake_root,
        instrument_id=SPY,
        observed_on=OBSERVED_ON,
        recorded_at=RECORDED,
        ex_date=EX_DATE,
        type=TYPE_SPLIT,
        split_ratio=2.0,
        provenance=PROVENANCE_OBSERVED,
    )
    assert sorted(actions.latest(lake_root)) == [
        (SPY, EX_DATE, TYPE_DIVIDEND),
        (SPY, EX_DATE, TYPE_SPLIT),
    ]


# -- the point-in-time read ----------------------------------------------------


def test_the_as_of_read_returns_what_the_lake_knew_on_the_day(lake_root):
    # Without this read a backtest spanning June silently uses the August correction.
    _dividend(lake_root, cash_amount=1.90352, recorded_at=RECORDED)
    _dividend(lake_root, cash_amount=1.95, recorded_at=CORRECTED)
    key = (SPY, EX_DATE, TYPE_DIVIDEND)

    assert actions.as_of(lake_root, date(2026, 6, 20))[key]["cash_amount"] == 1.90352
    assert actions.as_of(lake_root, date(2026, 8, 6))[key]["cash_amount"] == 1.95
    # The correction's own market day counts, because the filter is the end of ``on``.
    assert actions.as_of(lake_root, date(2026, 8, 5))[key]["cash_amount"] == 1.95
    assert actions.as_of(lake_root, date(2026, 8, 4))[key]["cash_amount"] == 1.90352
    # Before anything was recorded, the lake knew nothing.
    assert actions.as_of(lake_root, date(2026, 6, 18)) == {}


def test_the_as_of_read_resolves_in_file_order_like_the_current_read(lake_root):
    # The backwards-clock fixture, run through the other resolver. Both reads answer from
    # the file's order, and only the filter differs between them.
    _dividend(lake_root, cash_amount=1.90352, recorded_at=CORRECTED)
    _dividend(lake_root, cash_amount=1.95, recorded_at=RECORDED)

    resolved = actions.as_of(lake_root, date(2026, 8, 6))
    assert resolved[(SPY, EX_DATE, TYPE_DIVIDEND)]["cash_amount"] == 1.95


def test_the_as_of_read_compares_in_market_time_rather_than_on_the_text(lake_root):
    # 2026-08-06 00:30 UTC is 2026-08-05 20:30 ET. Reading the stored text's own date
    # would put this entry a day later than the market saw it.
    _dividend(
        lake_root,
        cash_amount=1.95,
        recorded_at=datetime(2026, 8, 6, 0, 30, tzinfo=UTC),
    )
    key = (SPY, EX_DATE, TYPE_DIVIDEND)
    assert actions.as_of(lake_root, date(2026, 8, 5))[key]["cash_amount"] == 1.95


# -- the line rules ------------------------------------------------------------


def test_a_torn_trailing_line_is_discarded(lake_root):
    _dividend(lake_root, cash_amount=1.90352, recorded_at=RECORDED)
    path = actions_path(lake_root)
    with path.open("a") as handle:
        handle.write('{"instrument_id": 1, "ex_date": "2026-09-1')

    assert len(actions.read(lake_root)) == 1
    assert actions.latest(lake_root)[(SPY, EX_DATE, TYPE_DIVIDEND)]["cash_amount"] == 1.90352


def test_a_complete_line_naming_no_key_raises_at_its_position(lake_root):
    # A torn trailing line is a write that did not finish. A whole line in the body naming
    # no key is a record no reader can interpret, and every adjusted price is computed
    # through this file, so stepping over it would weaken every factor downstream.
    _dividend(lake_root, cash_amount=1.90352, recorded_at=RECORDED)
    path = actions_path(lake_root)
    with path.open("a") as handle:
        handle.write(json.dumps({"recorded_at": RECORDED.isoformat(), "cash_amount": 2.0}) + "\n")

    assert len(actions.read(lake_root)) == 2
    with pytest.raises(LedgerLineError) as raised:
        actions.latest(lake_root)
    assert raised.value.position == 2
    assert "instrument_id" in str(raised.value)
    with pytest.raises(LedgerLineError):
        actions.as_of(lake_root, date(2026, 12, 31))


def test_a_line_whose_instrument_id_is_a_bool_raises(lake_root):
    # JSON's ``true`` indexes as 1, so a permissive read would file this action under
    # instrument 1 rather than refusing it.
    path = actions_path(lake_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"instrument_id": True, "ex_date": EX_DATE, "type": TYPE_DIVIDEND}) + "\n"
    )
    with pytest.raises(LedgerLineError):
        actions.latest(lake_root)


def test_an_entry_with_an_unreadable_recorded_at_raises_from_the_as_of_read(lake_root):
    path = actions_path(lake_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "instrument_id": SPY,
                "ex_date": EX_DATE,
                "type": TYPE_DIVIDEND,
                "recorded_at": "sometime last June",
            }
        )
        + "\n"
    )
    with pytest.raises(LedgerLineError):
        actions.as_of(lake_root, date(2026, 12, 31))


# -- one event, one key --------------------------------------------------------


def test_an_entry_naming_no_recorded_at_raises_from_the_as_of_read(lake_root):
    # The as-of read is the one that needs the stamp, so it is where a line without one
    # has to stop rather than raising a bare KeyError with no position in it.
    _write_lines(lake_root, [{"instrument_id": SPY, "ex_date": EX_DATE, "type": TYPE_DIVIDEND}])
    with pytest.raises(LedgerLineError) as raised:
        actions.as_of(lake_root, date(2026, 12, 31))
    assert "recorded_at" in str(raised.value)


def test_an_entry_whose_recorded_at_is_naive_raises_from_the_as_of_read(lake_root):
    # ``append`` refuses a naive stamp, and a hand-written line is not written by
    # ``append``. Read in the machine's local zone it would land on whichever day that
    # machine happened to be in.
    _write_lines(
        lake_root,
        [
            {
                "instrument_id": SPY,
                "ex_date": EX_DATE,
                "type": TYPE_DIVIDEND,
                "recorded_at": "2026-08-05T19:00:00",
            }
        ],
    )
    with pytest.raises(LedgerLineError):
        actions.as_of(lake_root, date(2026, 12, 31))


def test_the_two_vendor_date_spellings_resolve_to_one_key(lake_root):
    # Schwab supplies ``div_ex_date`` as a timestamp spelling of a date. Unnormalized,
    # the correction below would land beside the original instead of superseding it, and
    # last-wins would return whichever was appended later.
    _dividend(lake_root, cash_amount=1.90352, recorded_at=RECORDED, ex_date="2026-06-18T00:00:00Z")
    _dividend(lake_root, cash_amount=1.95, recorded_at=CORRECTED, ex_date="2026-06-18")

    resolved = actions.latest(lake_root)
    assert list(resolved) == [(SPY, EX_DATE, TYPE_DIVIDEND)]
    assert resolved[(SPY, EX_DATE, TYPE_DIVIDEND)]["cash_amount"] == 1.95
    assert {entry["ex_date"] for entry in actions.read(lake_root)} == {EX_DATE}


# -- the schema stamp ----------------------------------------------------------


def test_an_entry_carrying_a_foreign_schema_version_is_read_rather_than_refused(lake_root):
    # The ``!=`` refusal is for a read-modify-write file, where code half understanding a
    # shape writes the misunderstanding back. A written line is never touched again, so an
    # old shape stays readable and the stamp is what interprets it.
    _dividend(lake_root, cash_amount=1.90352, recorded_at=RECORDED, schema_version=99)

    entries = actions.read(lake_root)
    assert [entry["schema_version"] for entry in entries] == [99]
    assert actions.latest(lake_root)[(SPY, EX_DATE, TYPE_DIVIDEND)]["cash_amount"] == 1.90352


# -- the entry that skips resolution -------------------------------------------


def test_an_entry_with_a_null_observed_on_carries_its_own_instrument_id(lake_root):
    # A human's entry has no observation to resolve at, so it supplies the id directly.
    # Nothing here writes one. The schema reserves the value for marketlake #286.
    entry = actions.append(
        lake_root,
        instrument_id=SPY,
        observed_on=None,
        recorded_at=CORRECTED,
        ex_date=EX_DATE,
        type=TYPE_DIVIDEND,
        cash_amount=1.95,
        provenance=PROVENANCE_MANUAL,
    )
    assert entry["observed_on"] is None
    assert entry["instrument_id"] == SPY

    resolved = actions.latest(lake_root)[(SPY, EX_DATE, TYPE_DIVIDEND)]
    assert resolved["observed_on"] is None
    assert resolved["provenance"] == PROVENANCE_MANUAL


# -- resolving the instrument --------------------------------------------------


def test_resolution_runs_at_the_observation_date_rather_than_the_ex_date(lake_root):
    # Onboarding sets ``valid_from`` to the onboarding date and ``Mapping.valid_on``
    # refuses any earlier day, so resolving at a pre-capture ex-date would hold every
    # historical action. The observation date always resolves.
    master = _master()
    assert actions.resolve_instrument(master, "SPY", OBSERVED_ON) == 1
    with pytest.raises(UnresolvedSymbol):
        actions.resolve_instrument(master, "SPY", date(2026, 6, 18))


def _land(lake_root: Path, master: SecurityMaster, symbol: str) -> dict:
    """Resolve then append, the order marketlake #284's extraction runs them in.

    The fail-closed tests below drive this rather than the resolver alone, so "writes
    nothing" is a claim about a path that actually tries to write.
    """
    instrument_id = actions.resolve_instrument(master, symbol, OBSERVED_ON)
    return _dividend(
        lake_root, cash_amount=1.90352, recorded_at=RECORDED, instrument_id=instrument_id
    )


def test_the_landing_path_writes_when_the_symbol_resolves(lake_root):
    # The control for the two tests below. Without it, a resolver that raised on every
    # symbol would pass both of them.
    _land(lake_root, _master(), "SPY")
    assert len(actions.read(lake_root)) == 1
    assert [entry["partition"] for entry in read_manifest(lake_root)] == [ACTIONS_PARTITION]


def test_an_unresolvable_symbol_fails_closed_and_writes_nothing(lake_root):
    # A quote row exists only inside a capture span, so a symbol the lake observed and the
    # master cannot place means the master and the capture spans disagree.
    with pytest.raises(UnresolvedSymbol):
        _land(lake_root, _master(), "QQQ")

    assert not actions_path(lake_root).exists()
    assert read_manifest(lake_root) == []


def test_an_ambiguous_symbol_fails_closed_and_writes_nothing(lake_root):
    # One symbol mapping to several instruments on one date is what the master calls a
    # corrupt master. An action held out can be landed later. One landed under the wrong
    # instrument corrupts every factor that instrument's prices feed.
    master = _master()
    master.register(
        kind="equity",
        capture_start=datetime(2026, 9, 8, 13, 30, tzinfo=UTC),
        valid_from=date(2026, 9, 8),
        ticker="SPY",
    )
    with pytest.raises(AmbiguousSymbol):
        _land(lake_root, master, "SPY")

    assert not actions_path(lake_root).exists()
    assert read_manifest(lake_root) == []


# -- the manifest and the lock -------------------------------------------------


def test_the_write_refreshes_the_manifest_entry_and_the_scrub_passes(lake_root):
    # ``actions/`` is not in the reverse scrub's exclusion set, so a ledger written
    # without its entry would be reported as an orphan.
    _dividend(lake_root, cash_amount=1.90352, recorded_at=RECORDED)
    _dividend(lake_root, cash_amount=1.95, recorded_at=CORRECTED)

    entries = read_manifest(lake_root)
    assert [entry["partition"] for entry in entries] == [ACTIONS_PARTITION, ACTIONS_PARTITION]
    current = latest_entries(lake_root)[ACTIONS_PARTITION]
    assert current["source"] == SWEEP_SOURCE
    assert current["rows"] == 2
    assert current["fetched_at"] == CORRECTED.isoformat()
    assert scrub(lake_root).ok


def test_the_fourth_source_value_is_new_to_the_manifest(lake_root):
    # The live manifest uses ``capture``, ``compaction`` and ``reference``. The rest of
    # D16 imports this one rather than restating the string.
    assert SWEEP_SOURCE not in {"capture", "compaction", "reference"}


def test_the_manifest_entry_is_written_while_the_lock_is_still_held(lake_root, monkeypatch):
    # The blocked-writer test below proves the pair starts under the lock. It cannot see
    # the manifest write being dedented out of the hold, because a writer blocked at the
    # ``with`` writes neither line either way. This one asks the question directly: at the
    # moment ``record_partition`` runs, is the lake-root lock still held? A second open of
    # the manifest conflicts with an existing ``flock`` even inside one process, so trying
    # for it non-blocking answers that.
    held: list[bool] = []
    real = actions.record_partition

    def spy(*args, **kwargs):
        fd = os.open(lake_root / "manifest.jsonl", os.O_RDONLY | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
            held.append(False)
        except BlockingIOError:
            held.append(True)
        finally:
            os.close(fd)
        return real(*args, **kwargs)

    monkeypatch.setattr(actions, "record_partition", spy)
    _dividend(lake_root, cash_amount=1.90352, recorded_at=RECORDED)
    assert held == [True]


def test_the_entry_and_its_manifest_line_are_written_under_the_lake_lock(lake_root):
    # Every lake-mutating job takes the one lake-root flock first. A writer that skipped it
    # would land both lines while this test holds the lock, which is what the assertions
    # inside the ``with`` block catch. Holding them together is what keeps a weekend write
    # from leaving the Sunday scrub facing a sha nothing has caught up to.
    failures: list[BaseException] = []
    done = threading.Event()

    def run() -> None:
        try:
            _dividend(lake_root, cash_amount=1.90352, recorded_at=RECORDED)
        except BaseException as exc:  # noqa: BLE001 - reported to the main thread below
            failures.append(exc)
        finally:
            done.set()

    worker = threading.Thread(target=run)
    with lake_lock(lake_root):
        worker.start()
        # Long enough for the worker to reach the lock and block on it.
        time.sleep(0.3)
        assert actions.read(lake_root) == []
        assert read_manifest(lake_root) == []
        assert not done.is_set()

    assert done.wait(10)
    worker.join(10)
    assert failures == []
    assert len(actions.read(lake_root)) == 1
    assert [entry["partition"] for entry in read_manifest(lake_root)] == [ACTIONS_PARTITION]


def test_a_bad_line_names_its_position_in_the_file_not_in_the_filtered_read(lake_root):
    # ``as_of`` resolves a subset, so a position counted over that subset would send a
    # reader to the wrong line of the file. The two entries above the bad one are recorded
    # in August, so the June read drops both and the bad line is the only one it keeps.
    _dividend(lake_root, cash_amount=1.90352, recorded_at=CORRECTED)
    _dividend(lake_root, cash_amount=1.92, recorded_at=CORRECTED)
    with actions_path(lake_root).open("a") as handle:
        handle.write(json.dumps({"recorded_at": RECORDED.isoformat(), "cash_amount": 2.0}) + "\n")

    with pytest.raises(LedgerLineError) as raised:
        actions.as_of(lake_root, date(2026, 6, 20))
    assert raised.value.position == 3


# -- a ledger that was damaged before this write ---------------------------------------


def test_an_append_after_a_torn_line_keeps_the_writer_working(lake_root):
    # A torn fragment costs the entries after it in every read, and it costs the entry
    # appended straight onto its open line. That is the manifest's line rule, and a guard
    # that read the file before writing broke the single-write atomicity the rule rests
    # on. What must not happen is the writer stopping. Counting the manifest's rows from
    # what ``read`` returns would drop the count below the manifested one, so
    # ``guard_row_count`` would raise on this append and every append after it, each one
    # having already written its line.
    _dividend(lake_root, cash_amount=1.90352, recorded_at=RECORDED)
    _dividend(lake_root, cash_amount=1.92, recorded_at=RECORDED)
    path = actions_path(lake_root)
    path.write_text(path.read_text()[:-30])

    _dividend(lake_root, cash_amount=1.95, recorded_at=CORRECTED)
    _dividend(lake_root, cash_amount=1.97, recorded_at=CORRECTED)

    lines = [line for line in path.read_text().splitlines() if line.strip()]
    assert json.loads(lines[-1])["cash_amount"] == 1.97
    assert latest_entries(lake_root)[ACTIONS_PARTITION]["rows"] == len(lines)
    # The count the manifest carries is above what the damaged file reads back, which is
    # the signal that the ledger needs a human.
    assert len(actions.read(lake_root)) < latest_entries(lake_root)[ACTIONS_PARTITION]["rows"]


def _damage(path: Path, needle: bytes, replacement: bytes) -> None:
    """Change one byte of an already written ledger, the way bit rot or a hand edit does."""
    raw = path.read_bytes()
    assert raw.count(needle) == 1, "the fixture no longer says what it meant to"
    path.write_bytes(raw.replace(needle, replacement))


def test_a_ledger_that_is_not_utf8_refuses_as_an_actions_error(lake_root):
    """Site 2 of marketlake #499, and ``manifest.LedgerNotUtf8``'s sibling on this file.

    ``read_text`` decoded strictly, so ``UnicodeDecodeError`` came out of here. It is a
    ``ValueError`` and therefore not an ``ActionsError``, so it reached neither
    ``sweep._LEDGER_REFUSALS`` nor ``sweep._BARS_REFUSALS`` and ended the whole 18:30 run.

    The class is what matters more than the name. Both tuples name ``ActionsError`` rather than
    one of its members, which marketlake #497 made true, so this sibling was contained on the
    day it was written and no tuple anywhere had to be widened. That is asserted here rather
    than assumed, because the containment is what the refusal is for.
    """
    _dividend(lake_root, cash_amount=1.60, recorded_at=RECORDED)
    _damage(actions_path(lake_root), b'"dividend"', b'"dividen\xff"')

    with pytest.raises(actions.LedgerNotUtf8) as refusal:
        actions.read(lake_root)

    assert isinstance(refusal.value, actions.ActionsError)
    assert not isinstance(refusal.value, ValueError), "a ValueError is what escaped every tuple"
    assert str(actions_path(lake_root)) in str(refusal.value)

    from lake.sweep import _BARS_REFUSALS, _LEDGER_REFUSALS

    assert isinstance(refusal.value, _LEDGER_REFUSALS)
    assert isinstance(refusal.value, _BARS_REFUSALS)


def test_the_actions_refusal_names_the_byte_and_the_line_a_repair_has_to_find(lake_root):
    """The number's whole job is to send the person repairing the file to the right place.

    ``UnicodeDecodeError`` carries a byte offset alone, which is the wrong unit for an editor,
    so the line is counted from the newlines in front of it. Two whole entries land first, so a
    line number taken from the entries rather than the bytes would read 1 here.
    """
    _dividend(lake_root, cash_amount=1.60, recorded_at=RECORDED)
    _dividend(lake_root, cash_amount=1.65, recorded_at=CORRECTED, ex_date="2026-06-19")
    _damage(actions_path(lake_root), b'"2026-06-19"', b'"2026-06-\xff9"')

    with pytest.raises(actions.LedgerNotUtf8) as refusal:
        actions.read(lake_root)

    message = str(refusal.value)
    assert "on line 2" in message, message
    assert "0xff" in message, message
    # Derived from the fixture rather than typed, so a mutant hard-coding the number fails.
    offset = actions_path(lake_root).read_bytes().index(b"\xff")
    assert f"byte {offset} " in message, message
    assert "invalid start byte" in message, message
    assert "writes a byte outside ASCII" in message, message
    assert "human's job under the lock" in message, message
    # The consequence names this ledger's loss. ``manifest``'s sentence is about the partitions
    # a quarantine ledger withholds, and this file withholds nothing.
    assert "adjustments" in message, message
    assert "withholds" not in message, message


def test_the_actions_refusal_is_not_a_replacement_because_replacing_mangles_the_key(lake_root):
    """Why this refuses instead of decoding with ``errors="replace"``, which is one line.

    A replacement character inside a JSON string leaves the line **valid JSON** with one field
    silently rewritten, and every field this ledger resolves on is part of the key. A byte
    flipped inside ``ex_date`` files the action under a key no reader asks for, so the event it
    records adjusts nothing and ``load_bars`` hands back an as-traded price under an adjusted
    name. The assertions below are what a replacement would produce, stated as what must not
    happen.
    """
    _dividend(lake_root, cash_amount=1.60, recorded_at=RECORDED)
    _damage(actions_path(lake_root), b'"2026-06-18"', b'"2026-06-\xff8"')

    with pytest.raises(actions.LedgerNotUtf8):
        actions.latest(lake_root)

    replaced = actions_path(lake_root).read_bytes().decode("utf-8", "replace")
    entry = json.loads(replaced.splitlines()[0])
    assert entry["ex_date"] != "2026-06-18", "the premise of this test no longer holds"
    assert entry["type"] == TYPE_DIVIDEND


def test_a_hand_repaired_actions_ledger_may_hold_non_ascii_and_still_reads(lake_root):
    """The accepting side of the boundary, which is where narrowing it would do the damage.

    No writer here emits a byte outside ASCII, so decoding as ASCII would pass every test the
    tree has and still refuse a ledger a human had just repaired. Repairing this file is a hand
    edit under the lock, and a person writes the characters their language has.
    """
    entry = {"instrument_id": 1, "ex_date": "2026-06-18", "type": TYPE_DIVIDEND, "note": "café"}
    actions_path(lake_root).parent.mkdir(parents=True, exist_ok=True)
    actions_path(lake_root).write_bytes(
        (json.dumps(entry, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")
    )

    assert max(actions_path(lake_root).read_bytes()) > 127, "the fixture stopped being the case"
    assert actions.read(lake_root)[0]["note"] == "café"


def test_the_count_answers_on_a_ledger_the_read_refuses_and_never_falls(lake_root):
    """Site 3 of marketlake #499, and the half that must **not** refuse.

    This count sits inside ``append``, after ``append_line`` has landed the line and before
    ``record_partition`` records the entry describing it. A raise between those two leaves the
    ledger a line longer than its manifest entry, which is marketlake #523, so the count keeps
    its direct read and answers where the reader above refuses.

    The number is what the count always returned. A replacement substitutes inside a line and
    never changes how many lines there are, which is the property counting over the raw bytes
    would lose.
    """
    _dividend(lake_root, cash_amount=1.60, recorded_at=RECORDED)
    _dividend(lake_root, cash_amount=1.65, recorded_at=CORRECTED, ex_date="2026-06-19")
    assert actions.entry_line_count(lake_root) == 2

    _damage(actions_path(lake_root), b'"2026-06-19"', b'"2026-06-\xff9"')

    with pytest.raises(actions.LedgerNotUtf8):
        actions.read(lake_root)
    assert actions.entry_line_count(lake_root) == 2, "the count stopped answering"


def test_counting_over_the_raw_bytes_would_let_the_count_fall(lake_root):
    """Why the count decodes rather than counting newlines over ``read_bytes``.

    ``bytes.splitlines`` splits on fewer separators than ``str.splitlines``, so a hand edit
    holding ``U+2028`` inside a field counts one line over the bytes where it counts two over
    the text. A count that falls is exactly what ``manifest.guard_row_count`` raises on, and
    the raise would land after ``append_line`` had already written the line.

    The byte count is computed here rather than described, so the gap this avoids is a
    measurement rather than a claim.
    """
    raw = (
        json.dumps({"instrument_id": 1, "ex_date": "2026-06-18", "type": TYPE_DIVIDEND}) + "\n"
    ).encode("utf-8") + (
        json.dumps({"instrument_id": 2, "note": "a b"}, ensure_ascii=False) + "\n"
    ).encode("utf-8")
    actions_path(lake_root).parent.mkdir(parents=True, exist_ok=True)
    actions_path(lake_root).write_bytes(raw)

    over_bytes = sum(1 for line in raw.splitlines() if line.strip())
    assert over_bytes == 2, "the fixture stopped separating the two counts"
    assert actions.entry_line_count(lake_root) == 3


def test_both_actions_reads_pin_utf8_rather_than_the_locales_encoding(lake_root, monkeypatch):
    """``read_text`` with no argument decodes in the locale's encoding, and neither read does.

    Python 3.12 turns UTF-8 mode on by itself under a C locale, so a hostile locale cannot be
    set from inside this process, and tests here never shell out. What holds the pinning instead
    is the method neither read may call: patched to raise, a revert to ``read_text`` fails here
    rather than passing on a machine whose locale happens to be UTF-8.

    Both reads are driven, because they were fixed for opposite reasons and a test covering one
    would leave the other free to go back.
    """
    entry = {"instrument_id": 1, "ex_date": EX_DATE, "type": TYPE_DIVIDEND, "note": "café"}
    actions_path(lake_root).parent.mkdir(parents=True, exist_ok=True)
    actions_path(lake_root).write_bytes(
        (json.dumps(entry, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")
    )

    def refuse(*args, **kwargs):  # pragma: no cover - neither call may reach it
        raise AssertionError("the ledger was read in the locale's encoding")

    monkeypatch.setattr(Path, "read_text", refuse)
    assert actions.read(lake_root)[0]["note"] == "café"
    assert actions.entry_line_count(lake_root) == 1
