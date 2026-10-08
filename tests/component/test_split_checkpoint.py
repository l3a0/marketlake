"""The split checkpoint: who resumes from it, who is refused, and how the 18:30 sweep writes it.

marketlake #755 trims old chains partitions from the hosted VM, and a split walk rebuilt from the
first day left on disk lands permanent phantom splits. marketlake #783 let the walk resume from a
saved state. marketlake #786 saves the states in ``reference/split_checkpoint.parquet``, resumes a
ticker from it only when the ticker has a designed absence, and refuses a ticker where a resume
would be wrong.

**How these tests trim.** The way marketlake #787 will: the partition file is unlinked and a trim
line naming its manifest sha is appended to ``trimmed.jsonl``, and the manifest entry stays. That
is what makes the absence designed rather than lost.
"""

from __future__ import annotations

import fcntl
import os
import shutil
from dataclasses import replace
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import actions, split_checkpoint, splits, sweep
from lake.alert import Publisher
from lake.config import GuardConstants
from lake.lock import lake_lock
from lake.manifest import (
    ManifestError,
    append_quarantine,
    latest_entries,
    record_partition,
    sha256_file,
)
from lake.paths import CHAINS, TRIMMED_FILE, LakePaths
from lake.report import ACTION, SPLITS_PIECE
from lake.security_master import ID_TYPE_FIGI, SecurityMaster, master_path
from lake.split_checkpoint import (
    CHECKPOINT_PARTITION,
    REPAIR,
    Checkpoint,
    CheckpointEntry,
    CheckpointUnreadable,
    checkpoint_path,
    mappings_at,
    read_checkpoint,
    starting_state,
    walk_splits,
    write_checkpoint,
)
from lake.splits import REASON_PARTITION_ABSENT, WalkState
from lake.trimmed import append_trimmed, restore_line, trim_line
from tests.component.test_eod_sweep import (
    EVENING,
    MONDAY,
    NEXT_MONDAY,
    PING_URL,
    SESSION,
    _CountingVendorSource,
    _RecordingSetter,
    _roster,
    _schedule_text,
)
from tests.component.test_eod_sweep import _lake as _sweep_lake
from tests.component.test_split_detection import (
    CALENDAR,
    CARRIED_OCC,
    DAY_ONE,
    DAY_THREE,
    DAY_TWO,
    FIRST_NIGHT,
    SECOND_NIGHT,
    _adjusted_row,
    _entries,
    _lake,
    _mapping,
    _row,
)
from tests.component.test_split_resume import RETURNING
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.lake import FixtureLake, sample_chains_table
from tests.support.pinger import FakePinger
from tests.support.transport import FakeTransport

# A root on every row of day one, gone on day two and back on day three. Read from day one on,
# the return is counted rather than landed. Read from day two on, it is a gain, and the 1.5
# deliverable the returning contracts carry lands a phantom split.
RETURNING_LAKE = {
    ("SPY", DAY_ONE): [_row(DAY_ONE), _row(DAY_ONE, **RETURNING)],
    ("SPY", DAY_TWO): [_row(DAY_TWO)],
    ("SPY", DAY_THREE): [_row(DAY_THREE), _row(DAY_THREE, **RETURNING)],
}

# A real split on day two that stays visible on day three. A walk that reads day two again
# re-derives it and counts it as unchanged. A walk resumed after day two never reads it.
SPLIT_LAKE = {
    ("SPY", DAY_ONE): [_row(DAY_ONE)],
    ("SPY", DAY_TWO): [_row(DAY_TWO, occ_symbol=CARRIED_OCC), _adjusted_row(DAY_TWO)],
    ("SPY", DAY_THREE): [_row(DAY_THREE, occ_symbol=CARRIED_OCC), _adjusted_row(DAY_THREE)],
}

STAMP = "2026-09-16T20:31:00+00:00"


def _partition(day: date, ticker: str = "SPY") -> str:
    return f"{CHAINS}/ticker={ticker}/date={day.isoformat()}.parquet"


def _trim(root: Path, *days: date, ticker: str = "SPY", unlink: bool = True) -> None:
    """Trim each day the way marketlake #787 will: a trim line, then the unlink."""
    manifest = latest_entries(root)
    with lake_lock(root):
        for day in days:
            partition = _partition(day, ticker)
            line = trim_line(
                partition,
                sha256=manifest[partition]["sha256"],
                version_id="v1",
                verified_at=STAMP,
                trimmed_at=STAMP,
            )
            append_trimmed(root, line, source="trim", fetched_at=None)
            if unlink:
                (root / partition).unlink()


def _restore(root: Path, day: date, saved: bytes | None) -> None:
    """A restore line over a trim, with the file put back when ``saved`` holds its bytes."""
    partition = _partition(day)
    manifest = latest_entries(root)
    with lake_lock(root):
        if saved is not None:
            (root / partition).write_bytes(saved)
        line = restore_line(partition, sha256=manifest[partition]["sha256"], restored_at=STAMP)
        append_trimmed(root, line, source="restore", fetched_at=None)


def _night_one(root: Path, *, edge: date) -> Checkpoint:
    """The first night's walk, cut at ``edge``, and the checkpoint the sweep would write."""
    walked = walk_splits(
        lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR, edge=edge
    )
    checkpoint = Checkpoint(session_day=DAY_TWO, entries=walked.entries)
    write_checkpoint(root, checkpoint, recorded_at=FIRST_NIGHT)
    return checkpoint


def _night_two(root: Path, *, edge: date | None = None):
    return walk_splits(
        lake_root=root, clock=ManualClock(SECOND_NIGHT), calendar=CALENDAR, edge=edge
    )


def _splits(root: Path) -> list[dict]:
    return [entry for entry in _entries(root) if entry["type"] == "split"]


def _refused(walked) -> dict[str, str]:
    return {refusal.ticker: refusal.reason for refusal in walked.report.refused}


def _state(walked, ticker: str = "SPY") -> WalkState | None:
    return next((state for state in walked.report.states if state.ticker == ticker), None)


# -- the file ----------------------------------------------------------------------------------


def test_a_checkpoint_reads_back_exactly_as_written(fixture_lake: FixtureLake):
    """Every field of every state comes back, at the type it was read, the session rows included.

    ``==`` alone would pass an ``ssid`` read back as ``650.0`` or a flag as ``1``, so each saved
    row's values are compared by type too.
    """
    root = _lake(fixture_lake, SPLIT_LAKE)
    written = _night_one(root, edge=DAY_TWO)

    read = read_checkpoint(root)

    assert read == written
    (entry,) = read.entries
    assert entry.state.previous is not None
    assert entry.state.history
    assert entry.mappings
    original = written.entries[0].state.previous.rows
    for (root_read, row_read), (root_written, row_written) in zip(
        entry.state.previous.rows, original, strict=True
    ):
        assert root_read == root_written
        assert {k: type(v) for k, v in row_read.items()} == {
            k: type(v) for k, v in row_written.items()
        }


def test_the_checkpoint_is_recorded_in_the_manifest_with_its_bytes(fixture_lake: FixtureLake):
    root = _lake(fixture_lake, SPLIT_LAKE)
    _night_one(root, edge=DAY_TWO)

    entry = latest_entries(root)[CHECKPOINT_PARTITION]
    assert entry["sha256"] == sha256_file(checkpoint_path(root))
    assert entry["rows"] == 1
    assert entry["source"] == "reference"
    assert not list(checkpoint_path(root).parent.glob("*.tmp-*"))


def test_an_absent_checkpoint_reads_as_none(fixture_lake: FixtureLake):
    assert read_checkpoint(_lake(fixture_lake, SPLIT_LAKE)) is None


def test_an_empty_checkpoint_is_not_written(fixture_lake: FixtureLake):
    root = _lake(fixture_lake, SPLIT_LAKE)

    with pytest.raises(ValueError):
        write_checkpoint(root, Checkpoint(session_day=DAY_TWO, entries=()), recorded_at=FIRST_NIGHT)

    assert not checkpoint_path(root).exists()


# -- 1. who resumes ---------------------------------------------------------------------------


def test_a_trimmed_ticker_resumes_from_its_checkpoint_and_lands_no_phantom_split(
    fixture_lake: FixtureLake,
):
    """With day one trimmed, only the saved ``seen`` knows the returning root is not new."""
    root = _lake(fixture_lake, RETURNING_LAKE)
    _night_one(root, edge=DAY_ONE)
    _trim(root, DAY_ONE)

    walked = _night_two(root)

    assert walked.report.refused == ()
    assert _splits(root) == []
    assert _state(walked).cutoff == DAY_THREE


def test_a_ticker_whose_trims_are_all_restored_walks_from_scratch(fixture_lake: FixtureLake):
    """A ledger that names the ticker is not a designed absence once every trim is restored.

    Resumed, the walk would never read day two again and would count the split nowhere. From
    scratch it re-derives the split and counts it unchanged.
    """
    root = _lake(fixture_lake, SPLIT_LAKE)
    _night_one(root, edge=DAY_TWO)
    assert len(_splits(root)) == 1
    saved = {day: (root / _partition(day)).read_bytes() for day in (DAY_ONE, DAY_TWO)}
    _trim(root, DAY_ONE, DAY_TWO)
    for day, data in saved.items():
        _restore(root, day, data)

    walked = _night_two(root)

    assert walked.report.refused == ()
    assert walked.report.unchanged == 1


def test_the_same_lake_trimmed_and_not_restored_resumes(fixture_lake: FixtureLake):
    """The other side of the test above, so its count is the resume's doing and not the lake's."""
    root = _lake(fixture_lake, SPLIT_LAKE)
    _night_one(root, edge=DAY_TWO)
    _trim(root, DAY_ONE, DAY_TWO)

    walked = _night_two(root)

    assert walked.report.refused == ()
    assert walked.report.unchanged == 0


def test_a_restore_line_over_a_partition_lost_again_walks_from_scratch(
    fixture_lake: FixtureLake,
):
    """A partition the ledger restored and the disk lost is lost, not trimmed on purpose."""
    root = _lake(fixture_lake, SPLIT_LAKE)
    _night_one(root, edge=DAY_TWO)
    _trim(root, DAY_ONE)
    _restore(root, DAY_ONE, None)

    walked = _night_two(root)

    assert walked.report.refused == ()
    assert ("SPY", DAY_ONE, REASON_PARTITION_ABSENT) in {
        (skip.ticker, skip.day, skip.reason) for skip in walked.report.skipped
    }


def test_a_trim_line_beside_a_file_still_present_walks_from_scratch(fixture_lake: FixtureLake):
    """A crash between the trim line and the unlink leaves the file, and the file is read."""
    root = _lake(fixture_lake, SPLIT_LAKE)
    _night_one(root, edge=DAY_TWO)
    _trim(root, DAY_ONE, DAY_TWO, unlink=False)

    walked = _night_two(root)

    assert walked.report.refused == ()
    assert walked.report.unchanged == 1


def test_a_lake_with_no_trim_walks_every_ticker_as_before(fixture_lake: FixtureLake):
    """The laptop's lake: no ledger, so the walk is ``detect_splits`` with no saved state."""
    root = _lake(fixture_lake, SPLIT_LAKE)
    copy = root.parent / "copy"
    shutil.copytree(root, copy)

    walked = walk_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)
    plain = splits.detect_splits(lake_root=copy, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert walked.report.render() == plain.render()
    assert walked.report.states == plain.states
    assert _entries(root) == _entries(copy)


# -- 2. the hand command -----------------------------------------------------------------------


def test_the_hand_command_resumes_on_a_trimmed_lake(fixture_lake: FixtureLake, tmp_path: Path):
    """``python -m lake.actions splits`` goes through the same resume as the sweep."""
    root = _lake(fixture_lake, RETURNING_LAKE)
    _night_one(root, edge=DAY_ONE)
    _trim(root, DAY_ONE)

    code = actions.main(
        ["splits", "--config", str(write_config(tmp_path, lake_root=root))],
        clock=ManualClock(SECOND_NIGHT),
    )

    assert code == 0
    assert _splits(root) == []


def test_the_hand_command_prints_a_refused_ticker_and_exits_1(
    fixture_lake: FixtureLake, tmp_path: Path, capsys
):
    root = _lake(fixture_lake, RETURNING_LAKE)
    _trim(root, DAY_ONE)

    code = actions.main(
        ["splits", "--config", str(write_config(tmp_path, lake_root=root))],
        clock=ManualClock(SECOND_NIGHT),
    )

    assert code == 1
    out = capsys.readouterr().out
    assert "refused:   1" in out
    assert "SPY: its chains days are trimmed and the split checkpoint holds no entry" in out
    assert _splits(root) == []


# -- 3. each refusal -------------------------------------------------------------------------


def test_a_trimmed_ticker_with_no_checkpoint_is_refused(fixture_lake: FixtureLake):
    root = _lake(fixture_lake, RETURNING_LAKE)
    _trim(root, DAY_ONE)

    walked = _night_two(root)

    assert "holds no entry for it" in _refused(walked)["SPY"]
    assert _state(walked) is None
    assert _splits(root) == []


def test_a_trimmed_ticker_the_checkpoint_has_no_entry_for_is_refused(fixture_lake: FixtureLake):
    root = _lake(fixture_lake, RETURNING_LAKE)
    other = CheckpointEntry(
        state=WalkState("QQQ", DAY_ONE, None, frozenset(), (), 0, DAY_ONE), mappings=()
    )
    write_checkpoint(root, Checkpoint(DAY_TWO, (other,)), recorded_at=FIRST_NIGHT)
    _trim(root, DAY_ONE)

    walked = _night_two(root)

    assert "holds no entry for it" in _refused(walked)["SPY"]
    assert _splits(root) == []


def test_a_torn_checkpoint_refuses_a_trimmed_ticker(fixture_lake: FixtureLake):
    root = _lake(fixture_lake, RETURNING_LAKE)
    _night_one(root, edge=DAY_ONE)
    _trim(root, DAY_ONE)
    checkpoint_path(root).write_bytes(b"PAR1 torn")

    walked = _night_two(root)

    assert "split checkpoint cannot be read (CheckpointUnreadable)" in _refused(walked)["SPY"]
    assert walked.blocked
    assert _splits(root) == []


def test_a_day_trimmed_after_the_cutoff_is_refused(fixture_lake: FixtureLake):
    """Rule 4's first case: the resume would skip day two, which the saved state never read."""
    root = _lake(fixture_lake, RETURNING_LAKE)
    _night_one(root, edge=DAY_ONE)
    _trim(root, DAY_ONE, DAY_TWO)

    walked = _night_two(root)

    reason = _refused(walked)["SPY"]
    assert f"chains day {DAY_TWO.isoformat()} was trimmed after its checkpoint cutoff" in reason
    assert _splits(root) == []


def test_a_trim_that_lands_while_the_walk_runs_refuses_the_ticker(
    fixture_lake: FixtureLake, monkeypatch
):
    """Rule 4's second case: no designed absence at the start, one by the time day one is read.

    A sweep replayed at boot by ``Persistent=true`` can overlap a first trim this way. Walking
    on from scratch past the trimmed day would land the phantom split.
    """
    root = _lake(fixture_lake, RETURNING_LAKE)
    real = splits.read_session
    trimmed: list[date] = []

    def trimming(lake_root, ticker, day, instrument_id):
        if not trimmed:
            _trim(root, DAY_ONE)
            trimmed.append(DAY_ONE)
        return real(lake_root, ticker, day, instrument_id)

    monkeypatch.setattr(splits, "read_session", trimming)

    walked = _night_two(root)

    assert trimmed == [DAY_ONE]
    assert "was trimmed while the walk ran" in _refused(walked)["SPY"]
    assert _state(walked) is None
    assert _splits(root) == []


def test_an_ordinary_lost_partition_is_still_skipped_rather_than_refused(
    fixture_lake: FixtureLake,
):
    """The read-time check refuses only a designed absence. A lost file is skipped as before."""
    root = _lake(fixture_lake, SPLIT_LAKE)
    (root / _partition(DAY_ONE)).unlink()

    walked = _night_two(root)

    assert walked.report.refused == ()
    assert ("SPY", DAY_ONE, REASON_PARTITION_ABSENT) in {
        (skip.ticker, skip.day, skip.reason) for skip in walked.report.skipped
    }


def test_a_day_at_or_before_the_cutoff_now_quarantined_is_refused(fixture_lake: FixtureLake):
    """Rule 4's third case: a revoke or a whole-lake battery after the checkpoint was written."""
    root = _lake(fixture_lake, RETURNING_LAKE)
    _night_one(root, edge=DAY_TWO)
    _trim(root, DAY_ONE)
    append_quarantine(root, {"partition": _partition(DAY_TWO), "verdict": "bad"})

    walked = _night_two(root)

    reason = _refused(walked)["SPY"]
    assert f"chains day {DAY_TWO.isoformat()}, at or before its checkpoint cutoff" in reason
    assert "python -m lake.signoff" in reason


def test_a_back_dated_scope_edit_at_or_before_the_cutoff_is_refused(fixture_lake: FixtureLake):
    """Rule 4's fourth case: day one read clean, then a master edit put it out of scope.

    A whole-lake walk would meet day one as out of scope and widen the next boundary's window.
    A resume never reads day one again, so it has to refuse instead.
    """
    root = _lake(fixture_lake, RETURNING_LAKE)
    _night_one(root, edge=DAY_TWO)
    _trim(root, DAY_ONE)
    SecurityMaster([_mapping(1, "SPY", valid_from=DAY_TWO)]).write(master_path(root))

    walked = _night_two(root)

    assert "security master's mappings for it on or before" in _refused(walked)["SPY"]


def test_a_changed_instrument_at_the_saved_previous_day_is_refused(fixture_lake: FixtureLake):
    root = _lake(fixture_lake, RETURNING_LAKE)
    _night_one(root, edge=DAY_TWO)
    _trim(root, DAY_ONE)
    SecurityMaster([_mapping(7, "SPY")]).write(master_path(root))

    walked = _night_two(root)

    assert "security master's mappings for it on or before" in _refused(walked)["SPY"]


def test_a_rename_after_the_cutoff_does_not_refuse(fixture_lake: FixtureLake):
    """The mapping a rename closes ends after the cutoff, so it cuts to the same rows."""
    root = _lake(fixture_lake, RETURNING_LAKE)
    _night_one(root, edge=DAY_TWO)
    _trim(root, DAY_ONE)
    SecurityMaster(
        [_mapping(1, "SPY", valid_to=DAY_THREE), _mapping(1, "SPYX", valid_from=DAY_THREE)]
    ).write(master_path(root))

    walked = _night_two(root)

    assert walked.report.refused == ()


def test_a_ticker_sharing_its_instrument_with_another_is_refused(fixture_lake: FixtureLake):
    """Rule 4's fifth case: only a hand-corrupted master holds two tickers on one instrument."""
    root = _lake(fixture_lake, RETURNING_LAKE)
    _night_one(root, edge=DAY_TWO)
    _trim(root, DAY_ONE)
    SecurityMaster([_mapping(1, "SPY"), _mapping(1, "QQQ", valid_from=DAY_THREE)]).write(
        master_path(root)
    )

    walked = _night_two(root)

    assert "instrument 1 is also named by QQQ on overlapping days" in _refused(walked)["SPY"]


def test_a_quarantine_after_the_cutoff_does_not_refuse(fixture_lake: FixtureLake):
    """A day after the cutoff is one the resume reads, and skips, like a walk from scratch."""
    root = _lake(fixture_lake, RETURNING_LAKE)
    _night_one(root, edge=DAY_ONE)
    _trim(root, DAY_ONE)
    append_quarantine(root, {"partition": _partition(DAY_TWO), "verdict": "bad"})

    walked = _night_two(root)

    assert walked.report.refused == ()


def test_another_identifier_on_the_same_instrument_is_not_a_shared_ticker(
    fixture_lake: FixtureLake,
):
    """A FIGI names the instrument on the same days by design, and is not a second ticker."""
    root = _lake(fixture_lake, RETURNING_LAKE)
    _night_one(root, edge=DAY_TWO)
    _trim(root, DAY_ONE)
    figi = replace(_mapping(1, "BBG000BDTBL9"), id_type=ID_TYPE_FIGI)
    SecurityMaster([_mapping(1, "SPY"), figi]).write(master_path(root))

    walked = _night_two(root)

    assert walked.report.refused == ()


def test_a_ledger_damaged_during_the_walk_refuses_the_ticker_it_was_asked_about(
    fixture_lake: FixtureLake, monkeypatch
):
    """Whether the absent day was trimmed is unknown, so walking on past it is refused."""
    root = _lake(fixture_lake, RETURNING_LAKE)
    real = splits.read_session
    done: list[bool] = []

    def damaging(lake_root, ticker, day, instrument_id):
        if not done:
            _trim(root, DAY_ONE)
            ledger = root / TRIMMED_FILE
            ledger.write_bytes(b"\xef\xbb\xbf" + ledger.read_bytes())
            done.append(True)
        return real(lake_root, ticker, day, instrument_id)

    monkeypatch.setattr(splits, "read_session", damaging)

    walked = _night_two(root)

    assert "the trimmed ledger or the manifest cannot be read" in _refused(walked)["SPY"]
    assert _splits(root) == []


def test_a_state_passed_for_a_refused_ticker_is_not_reported(fixture_lake: FixtureLake):
    """``detect_splits`` drops a refused ticker's state, so the caller keeps its saved one."""
    root = _lake(fixture_lake, RETURNING_LAKE)
    state = WalkState("SPY", DAY_ONE, None, frozenset(), (), 0, DAY_ONE)

    report = splits.detect_splits(
        lake_root=root,
        clock=ManualClock(SECOND_NIGHT),
        calendar=CALENDAR,
        resume=[state],
        refused={"SPY": "refused by the caller"},
    )

    assert report.states == ()
    assert [(r.ticker, r.reason) for r in report.refused] == [("SPY", "refused by the caller")]


def _rewrite_checkpoint(root: Path, column: str, values: list) -> None:
    path = checkpoint_path(root)
    table = pq.read_table(path)
    index = table.schema.get_field_index(column)
    table = table.set_column(index, table.schema.field(index), pa.array(values, table[column].type))
    pq.write_table(table, path)


def _two_ticker_checkpoint(root: Path) -> None:
    entries = tuple(
        CheckpointEntry(state=WalkState(t, DAY_ONE, None, frozenset(), (), 0, DAY_ONE), mappings=())
        for t in ("QQQ", "SPY")
    )
    write_checkpoint(root, Checkpoint(DAY_TWO, entries), recorded_at=FIRST_NIGHT)


def test_a_checkpoint_naming_a_ticker_twice_is_unreadable(fixture_lake: FixtureLake):
    root = _lake(fixture_lake, SPLIT_LAKE)
    _two_ticker_checkpoint(root)
    _rewrite_checkpoint(root, "ticker", ["SPY", "SPY"])

    with pytest.raises(CheckpointUnreadable, match="names SPY twice"):
        read_checkpoint(root)


def test_a_checkpoint_naming_two_session_days_is_unreadable(fixture_lake: FixtureLake):
    root = _lake(fixture_lake, SPLIT_LAKE)
    _two_ticker_checkpoint(root)
    _rewrite_checkpoint(root, "session_day", [DAY_ONE, DAY_TWO])

    with pytest.raises(CheckpointUnreadable, match="one session day"):
        read_checkpoint(root)


def test_two_tickers_on_one_instrument_on_days_apart_are_not_refused(fixture_lake: FixtureLake):
    root = _lake(fixture_lake, RETURNING_LAKE)
    _night_one(root, edge=DAY_TWO)
    _trim(root, DAY_ONE)
    SecurityMaster([_mapping(1, "SPY"), _mapping(1, "QQQ", valid_to=date(2026, 9, 8))]).write(
        master_path(root)
    )

    walked = _night_two(root)

    assert walked.report.refused == ()


def test_the_saved_mappings_are_the_tickers_own_cut_at_the_cutoff():
    master = SecurityMaster(
        [
            _mapping(1, "SPY", valid_to=DAY_THREE),
            _mapping(2, "SPY", valid_from=DAY_THREE),
            _mapping(1, "QQQ", valid_from=DAY_ONE, valid_to=DAY_TWO),
        ]
    )

    assert mappings_at(master, "SPY", DAY_TWO) == ((1, "ticker", date(2026, 9, 8), None),)
    assert mappings_at(master, "SPY", DAY_THREE) == (
        (1, "ticker", date(2026, 9, 8), DAY_THREE),
        (2, "ticker", DAY_THREE, None),
    )


# -- 6. the cutoff after the window grows ----------------------------------------------------


def test_a_grown_window_keeps_a_resumed_tickers_cutoff_and_refuses_nothing(
    fixture_lake: FixtureLake,
):
    """The edge moves back past the saved cutoff, and the cutoff stays where it was."""
    root = _lake(fixture_lake, RETURNING_LAKE)
    _night_one(root, edge=DAY_TWO)
    _trim(root, DAY_ONE)

    walked = _night_two(root, edge=DAY_ONE)

    assert walked.report.refused == ()
    assert _state(walked).cutoff == DAY_TWO
    assert {entry.state.ticker: entry.state.cutoff for entry in walked.entries} == {"SPY": DAY_TWO}


# -- 10. a corrupt checkpoint ----------------------------------------------------------------


def test_a_corrupt_checkpoint_is_a_manifest_error(fixture_lake: FixtureLake):
    root = _lake(fixture_lake, SPLIT_LAKE)
    path = checkpoint_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"not parquet at all")

    with pytest.raises(CheckpointUnreadable) as raised:
        read_checkpoint(root)

    assert isinstance(raised.value, ManifestError)


def test_a_parquet_in_another_schema_is_unreadable(fixture_lake: FixtureLake):
    root = _lake(fixture_lake, SPLIT_LAKE)
    path = checkpoint_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"ticker": ["SPY"]}), path)

    with pytest.raises(CheckpointUnreadable):
        read_checkpoint(root)


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a file whatever its mode")
def test_a_checkpoint_that_cannot_be_opened_is_unreadable(fixture_lake: FixtureLake):
    """``read_reference_table`` lets an ``OSError`` through, so this module folds it."""
    root = _lake(fixture_lake, SPLIT_LAKE)
    _night_one(root, edge=DAY_TWO)
    path = checkpoint_path(root)
    path.chmod(0)
    try:
        with pytest.raises(CheckpointUnreadable, match="PermissionError"):
            read_checkpoint(root)
    finally:
        path.chmod(0o644)


# -- the sweep ----------------------------------------------------------------------------------

# Five weeks before ``SESSION`` and the two the sweep tests already use. The weeks of 2026-09-07
# stay out, as they do in ``test_eod_sweep``'s default calendar, so the capture span's first
# days are not sessions and the bar walk and the coverage check see what they always saw. The
# window edge for 22 sessions then falls on 2026-08-07.
LONG_CALENDAR = weekday_sessions(
    date(2026, 8, 3),
    date(2026, 8, 10),
    date(2026, 8, 17),
    date(2026, 8, 24),
    date(2026, 8, 31),
    MONDAY,
    NEXT_MONDAY,
)
EDGE = date(2026, 8, 7)
# A sealed SPY chains day before the master's 2026-09-08 start, so out of scope. It is the one
# day at or before the edge, which is what gives SPY a state to save.
EARLY = date(2026, 8, 3)


def _sweep(
    root: Path,
    *,
    window: int | str | None = 22,
    calendar=LONG_CALENDAR,
    pinger: FakePinger | None = None,
    transport: FakeTransport | None = None,
    guards: GuardConstants | None = None,
):
    pinger = FakePinger() if pinger is None else pinger
    transport = FakeTransport() if transport is None else transport
    outcome = sweep.sweep(
        lake_root=root,
        clock=ManualClock(EVENING),
        calendar=calendar,
        roster=_roster(),
        vendor_source=_CountingVendorSource(),
        pinger=pinger,
        ping_url=PING_URL,
        publisher=Publisher(lake_root=root, transport=transport, secrets=("secret-key",)),
        schedule_reader=lambda: _schedule_text(),
        schedule_setter=_RecordingSetter(),
        window_sessions=window,
        guards=guards,
    )
    return outcome, pinger


def _swept_lake(fixture_lake: FixtureLake, **kwargs) -> Path:
    chains = {("SPY", EARLY): sample_chains_table(), **kwargs.pop("chains", {})}
    return _sweep_lake(fixture_lake, chains=chains, **kwargs)


def test_the_sweep_writes_the_checkpoint_under_the_window_key(fixture_lake: FixtureLake):
    """It records tonight's session day, cuts each ticker at the edge, and still pings."""
    root = _swept_lake(fixture_lake)

    outcome, pinger = _sweep(root)

    checkpoint = read_checkpoint(root)
    assert checkpoint.session_day == SESSION
    assert checkpoint.cutoffs() == {"SPY": EARLY}
    entry = latest_entries(root)[CHECKPOINT_PARTITION]
    assert entry["sha256"] == sha256_file(checkpoint_path(root))
    assert outcome.nightly.problems == ()
    assert pinger.events


def test_a_host_without_the_window_key_writes_no_checkpoint(fixture_lake: FixtureLake):
    root = _swept_lake(fixture_lake)

    _sweep(root, window=None)

    assert not checkpoint_path(root).exists()
    assert CHECKPOINT_PARTITION not in latest_entries(root)


def test_a_holiday_writes_no_checkpoint(fixture_lake: FixtureLake):
    root = _swept_lake(fixture_lake)
    holiday = weekday_sessions(
        date(2026, 8, 3),
        date(2026, 8, 10),
        date(2026, 8, 17),
        date(2026, 8, 24),
        date(2026, 8, 31),
        MONDAY,
        NEXT_MONDAY,
        holidays=(SESSION,),
    )

    _sweep(root, calendar=holiday)

    assert not checkpoint_path(root).exists()


def test_a_window_under_the_floor_files_a_line_and_writes_no_checkpoint(
    fixture_lake: FixtureLake,
):
    """The render should have refused it. The sweep's backstop withholds nothing."""
    root = _swept_lake(fixture_lake)

    outcome, pinger = _sweep(root, window=21)

    assert not checkpoint_path(root).exists()
    assert any(line.startswith("lake window refused: ") for line in outcome.nightly.report)
    assert pinger.events


def test_an_edge_the_calendar_cannot_place_files_a_line_and_writes_no_checkpoint(
    fixture_lake: FixtureLake,
):
    root = _swept_lake(fixture_lake)

    outcome, _ = _sweep(root, calendar=weekday_sessions(MONDAY, NEXT_MONDAY))

    assert not checkpoint_path(root).exists()
    assert any(line.startswith("lake window edge not found: ") for line in outcome.nightly.report)


def test_a_refused_ticker_keeps_its_saved_entry_and_withholds_the_ping(fixture_lake: FixtureLake):
    """QQQ is refused, SPY still runs, and the digest says which ticker was refused.

    The checkpoint keeps QQQ's saved entry rather than being rewritten from tonight's partial
    result, so one night's refusal does not leave QQQ refused for ever.
    """
    root = _swept_lake(
        fixture_lake,
        tickers=("SPY", "QQQ"),
        instrument_ids=(1, 2),
        chains={("QQQ", SESSION): sample_chains_table()},
    )
    master = SecurityMaster.read(master_path(root))
    saved_day = date(2026, 9, 11)
    saved = Checkpoint(
        session_day=saved_day,
        entries=tuple(
            CheckpointEntry(
                state=WalkState(ticker, saved_day, None, frozenset(), (), 0, saved_day),
                mappings=mappings_at(master, ticker, saved_day),
            )
            for ticker in ("QQQ", "SPY")
        ),
    )
    write_checkpoint(root, saved, recorded_at=FIRST_NIGHT)
    _trim(root, SESSION, ticker="QQQ")
    transport = FakeTransport()

    outcome, pinger = _sweep(root, transport=transport)

    problems = outcome.nightly.problems
    assert len(problems) == 1
    assert problems[0].startswith("splits did not run for QQQ: chains day 2026-09-14 was trimmed")
    assert pinger.events == []
    assert dict(outcome.nightly.pieces)[SPLITS_PIECE].refusal is None
    assert "splits refused 1 ticker(s): QQQ" in outcome.digest.body
    written = read_checkpoint(root)
    assert written.session_day == SESSION
    assert written.entry("QQQ") == saved.entry("QQQ")
    assert written.entry("SPY") is not None


def test_a_torn_checkpoint_a_ticker_needs_is_left_for_the_repair(fixture_lake: FixtureLake):
    root = _swept_lake(
        fixture_lake,
        tickers=("SPY", "QQQ"),
        instrument_ids=(1, 2),
        chains={("QQQ", SESSION): sample_chains_table()},
    )
    _trim(root, SESSION, ticker="QQQ")
    path = checkpoint_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"torn checkpoint")

    outcome, pinger = _sweep(root)

    assert path.read_bytes() == b"torn checkpoint"
    assert any(
        problem.startswith("split checkpoint not written: ") for problem in outcome.nightly.problems
    )
    assert any(
        problem.startswith("splits did not run for QQQ: ") for problem in outcome.nightly.problems
    )
    assert pinger.events == []


def test_a_torn_checkpoint_no_ticker_needs_is_replaced_past_its_recorded_count(
    fixture_lake: FixtureLake,
):
    """The torn file's entry recorded more tickers than tonight's holds, and the write goes on."""
    root = _swept_lake(fixture_lake)
    path = checkpoint_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"torn checkpoint")
    record_partition(root, CHECKPOINT_PARTITION, source="reference", rows=99, fetched_at=None)

    outcome, pinger = _sweep(root)

    assert outcome.nightly.problems == ()
    assert read_checkpoint(root).cutoffs() == {"SPY": EARLY}
    assert latest_entries(root)[CHECKPOINT_PARTITION]["rows"] == 1
    assert pinger.events


def test_a_damaged_trimmed_ledger_refuses_the_splits_piece(fixture_lake: FixtureLake):
    """No ticker can be told to resume or to walk from scratch, so none is walked."""
    root = _swept_lake(fixture_lake)
    (root / TRIMMED_FILE).write_bytes(b"\xef\xbb\xbf" + b'{"partition": "x", "kind": "trim"}\n')

    outcome, pinger = _sweep(root)

    assert dict(outcome.nightly.pieces)[SPLITS_PIECE].refusal is not None
    assert pinger.events == []
    assert not checkpoint_path(root).exists()


def test_the_trimmed_partitions_stay_in_the_manifest(fixture_lake: FixtureLake):
    """What the tests here trim is what #787 trims: the file goes and its entry stays."""
    root = _lake(fixture_lake, RETURNING_LAKE)
    _trim(root, DAY_ONE)

    assert _partition(DAY_ONE) in latest_entries(root)
    assert not LakePaths(root).chains_partition_path("SPY", DAY_ONE).exists()


def test_the_hand_command_writes_no_checkpoint(fixture_lake: FixtureLake, tmp_path: Path):
    root = _lake(fixture_lake, SPLIT_LAKE)

    actions.main(
        ["splits", "--config", str(write_config(tmp_path, lake_root=root))],
        clock=ManualClock(FIRST_NIGHT),
    )

    assert not checkpoint_path(root).exists()


def test_a_checkpoint_entry_is_dated_by_the_night_it_ran(fixture_lake: FixtureLake):
    """The stamp on the entry is the sweep's own clock, so two nights record two entries."""
    root = _lake(fixture_lake, SPLIT_LAKE)
    _night_one(root, edge=DAY_TWO)

    assert latest_entries(root)[CHECKPOINT_PARTITION]["fetched_at"] == FIRST_NIGHT.isoformat()


# -- review fixes ---------------------------------------------------------------------------


def test_a_ticker_that_stops_before_its_first_day_saves_the_starting_state(
    fixture_lake: FixtureLake,
):
    """A saved cutoff must not outlive the day that now stops the walk before it.

    Day one is quarantined after the first night saved a cutoff of day two. Walked from
    scratch, the ticker stops before day one and reports no state. Kept, the saved entry would
    let #787 trim day two past a skip a sign-off can reverse. The entry becomes the state a walk
    from scratch starts from, whose cutoff covers no day, and the row count stays.
    """
    root = _lake(fixture_lake, RETURNING_LAKE)
    _night_one(root, edge=DAY_TWO)
    append_quarantine(root, {"partition": _partition(DAY_ONE), "verdict": "bad"})

    walked = _night_two(root, edge=DAY_THREE)

    assert _state(walked) is None
    (entry,) = walked.entries
    assert entry.state == starting_state("SPY")
    assert entry.state.cutoff < DAY_ONE
    write_checkpoint(root, Checkpoint(DAY_THREE, walked.entries), recorded_at=SECOND_NIGHT)
    assert read_checkpoint(root).cutoffs() == {"SPY": date.min}
    # ``last_day`` comes back ``None`` rather than ``date.min``. A resume from ``date.min``
    # would count every calendar day since year one as uncaptured. It is compared with the
    # literal, since both sides of an entry comparison move with a mutated starting state.
    (read,) = read_checkpoint(root).entries
    assert read.state.last_day is None
    assert read == entry
    assert _night_two(root, edge=DAY_THREE).report.refused == ()


def test_a_resumed_ticker_with_no_new_day_keeps_its_saved_entry(fixture_lake: FixtureLake):
    root = _lake(fixture_lake, RETURNING_LAKE)
    saved = _night_one(root, edge=DAY_THREE)
    _trim(root, DAY_ONE)

    walked = _night_two(root)

    assert walked.entries == saved.entries


def test_a_checkpoint_naming_one_contract_twice_refuses_in_the_sweep(fixture_lake: FixtureLake):
    """Valid Parquet whose history names one ssid twice is unreadable, not a ``ValueError``.

    Escaping the sweep would cost the night's report file, digest and ping.
    """
    root = _swept_lake(fixture_lake)
    master = SecurityMaster.read(master_path(root))
    state = WalkState("SPY", EARLY, None, frozenset(), ((1, "A", EARLY), (1, "B", EARLY)), 0, EARLY)
    entry = CheckpointEntry(state=state, mappings=mappings_at(master, "SPY", EARLY))
    write_checkpoint(root, Checkpoint(EARLY, (entry,)), recorded_at=FIRST_NIGHT)
    _trim(root, EARLY)

    with pytest.raises(CheckpointUnreadable, match="names one contract twice in SPY's history"):
        read_checkpoint(root)
    outcome, pinger = _sweep(root)

    assert outcome.filed_at is not None
    assert any(
        problem.startswith("splits did not run for SPY: ") for problem in outcome.nightly.problems
    )
    assert pinger.events == []


def test_the_window_key_in_config_yaml_reaches_the_checkpoint(
    fixture_lake: FixtureLake, capsys, monkeypatch, tmp_path: Path
):
    """``sweep_from_config`` hands the loaded key to the sweep, which then writes."""
    root = _swept_lake(fixture_lake)
    config = write_config(tmp_path, lake_root=root)
    with config.open("a") as handle:
        handle.write("lake_window_sessions: 22\n")
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("SPY:\n  options: true\n  bars:\n  - 1d\n")
    monkeypatch.setattr("lake.runner.UrllibPinger", FakePinger)
    monkeypatch.setattr("lake.alert.NtfyTransport", lambda topic: FakeTransport())
    monkeypatch.setattr(sweep, "ExchangeCalendar", lambda: LONG_CALENDAR)

    sweep.main(
        ["--config", str(config), "--tickers", str(tickers)],
        clock=ManualClock(EVENING),
        vendor_source=_CountingVendorSource(),
        schedule_setter=_RecordingSetter(),
        schedule_reader=lambda: _schedule_text(),
    )
    capsys.readouterr()

    assert read_checkpoint(root).cutoffs() == {"SPY": EARLY}


def test_a_checkpoint_write_that_raises_is_filed_and_withholds_the_ping(
    fixture_lake: FixtureLake, monkeypatch
):
    root = _swept_lake(fixture_lake)

    def failing(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(sweep, "write_checkpoint", failing)

    outcome, pinger = _sweep(root)

    assert outcome.nightly.problems == ("split checkpoint not written: OSError: disk full",)
    assert "split checkpoint not written: OSError" in outcome.nightly.report
    assert pinger.events == []
    assert outcome.filed_at is not None


def test_the_checkpoint_entry_is_recorded_while_the_lake_lock_is_held(
    fixture_lake: FixtureLake, monkeypatch
):
    """``record_partition`` takes no lock of its own, so the write has to hold it.

    A second open of the manifest conflicts with a held ``flock`` even inside one process, so
    trying for it without blocking says whether the lock is held.
    """
    root = _lake(fixture_lake, SPLIT_LAKE)
    held: list[bool] = []
    real = split_checkpoint.record_partition

    def spy(*args, **kwargs):
        fd = os.open(root / "manifest.jsonl", os.O_RDONLY | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
            held.append(False)
        except BlockingIOError:
            held.append(True)
        finally:
            os.close(fd)
        return real(*args, **kwargs)

    monkeypatch.setattr(split_checkpoint, "record_partition", spy)

    _night_one(root, edge=DAY_TWO)

    assert held == [True]


def test_the_sweep_judges_the_window_against_the_guards_it_was_given(fixture_lake: FixtureLake):
    """A raised trailing median raises the floor past 22, so the sweep writes nothing."""
    root = _swept_lake(fixture_lake)

    outcome, _ = _sweep(root, guards=GuardConstants(trailing_median_sessions=25))

    assert any(
        line.startswith("lake window refused: ") and "26 sessions" in line
        for line in outcome.nightly.report
    )
    assert not checkpoint_path(root).exists()


# -- the mutation lens's gaps ----------------------------------------------------------------


def test_the_sweep_cuts_at_exactly_the_window_edge(fixture_lake: FixtureLake):
    """Sealed days on the edge and the session after it pin the edge to one session."""
    root = _swept_lake(
        fixture_lake,
        chains={
            ("SPY", EDGE): sample_chains_table(),
            ("SPY", date(2026, 8, 10)): sample_chains_table(),
        },
    )

    _sweep(root)

    assert read_checkpoint(root).cutoffs() == {"SPY": EDGE}


def test_an_ordinary_write_keeps_the_row_count_guard(fixture_lake: FixtureLake):
    """Only the replacement of an unreadable file turns the guard off."""
    root = _swept_lake(fixture_lake)
    _sweep(root)
    record_partition(root, CHECKPOINT_PARTITION, source="reference", rows=99, fetched_at=None)

    outcome, _ = _sweep(root)

    assert any(
        problem.startswith("split checkpoint not written: RowCountRegression")
        for problem in outcome.nightly.problems
    )


def test_a_lake_with_no_chains_day_files_no_checkpoint_problem(fixture_lake: FixtureLake):
    root = _sweep_lake(fixture_lake, judged=())

    outcome, _ = _sweep(root)

    assert not checkpoint_path(root).exists()
    assert not any("split checkpoint" in problem for problem in outcome.nightly.problems)


@pytest.mark.parametrize(
    ("kwargs", "prefix"),
    [
        ({"window": 21}, "lake window refused: "),
        ({"calendar": weekday_sessions(MONDAY, NEXT_MONDAY)}, "lake window edge not found: "),
    ],
    ids=["refused", "edge not found"],
)
def test_the_window_lines_are_action_lines(fixture_lake: FixtureLake, kwargs, prefix: str):
    root = _swept_lake(fixture_lake)

    outcome, _ = _sweep(root, **kwargs)

    nightly = outcome.nightly
    (index,) = [i for i, line in enumerate(nightly.report) if line.startswith(prefix)]
    assert nightly.report_kinds[index] == ACTION


def test_a_blocked_write_puts_its_reason_in_the_report(fixture_lake: FixtureLake):
    root = _swept_lake(
        fixture_lake,
        tickers=("SPY", "QQQ"),
        instrument_ids=(1, 2),
        chains={("QQQ", SESSION): sample_chains_table()},
    )
    _trim(root, SESSION, ticker="QQQ")
    path = checkpoint_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"torn checkpoint")

    outcome, _ = _sweep(root)

    assert "split checkpoint not written: CheckpointUnreadable" in outcome.nightly.report


def test_the_sweep_stamps_the_checkpoint_entry_with_its_own_clock(fixture_lake: FixtureLake):
    root = _swept_lake(fixture_lake)

    _sweep(root)

    stamped = latest_entries(root)[CHECKPOINT_PARTITION]["fetched_at"]
    assert stamped == ManualClock(EVENING).now().isoformat()


def test_the_manifest_entry_counts_every_ticker(fixture_lake: FixtureLake):
    root = _lake(fixture_lake, RETURNING_LAKE)
    _two_ticker_checkpoint(root)

    assert latest_entries(root)[CHECKPOINT_PARTITION]["rows"] == 2


def test_a_state_with_an_unread_session_reads_back_exactly(fixture_lake: FixtureLake):
    """``unread_since`` above zero and ``previous`` before the cutoff come back at their types.

    The walks the other tests save only ever produce ``unread_since`` 0 and ``previous`` on the
    cutoff, so a reader that dropped either, or a column stored at a narrower type, would pass
    them. A spot of 655.37 has no exact ``float32``.
    """
    root = _lake(fixture_lake, RETURNING_LAKE)
    written = _night_one(root, edge=DAY_TWO)
    (entry,) = written.entries
    state = replace(
        entry.state,
        cutoff=DAY_THREE,
        last_day=DAY_THREE,
        unread_since=2,
        previous=replace(entry.state.previous, day=DAY_ONE, spot=655.37),
    )
    write_checkpoint(
        root, Checkpoint(DAY_THREE, (replace(entry, state=state),)), recorded_at=SECOND_NIGHT
    )

    (read,) = read_checkpoint(root).entries

    assert read.state == state
    assert read.state.previous.spot == 655.37
    assert type(read.state.previous.instrument_id) is int
    assert read.state.history
    assert {type(ssid) for ssid, _, _ in read.state.history} == {int}
    assert read.mappings
    assert {type(mapping[0]) for mapping in read.mappings} == {int}


@pytest.mark.parametrize("column", ["session_day", "cutoff"])
def test_a_checkpoint_row_missing_a_required_value_is_unreadable(
    fixture_lake: FixtureLake, column: str
):
    """A null cutoff would otherwise raise ``TypeError`` in the walk, outside the sweep's net."""
    root = _lake(fixture_lake, RETURNING_LAKE)
    _night_one(root, edge=DAY_TWO)
    _rewrite_checkpoint(root, column, [None])

    with pytest.raises(CheckpointUnreadable):
        read_checkpoint(root)


def test_a_failed_write_leaves_no_temp_file(fixture_lake: FixtureLake, monkeypatch):
    root = _lake(fixture_lake, RETURNING_LAKE)
    walked = walk_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    def failing(table, where):
        Path(where).write_bytes(b"partial")
        raise OSError("disk full")

    monkeypatch.setattr(split_checkpoint.pq, "write_table", failing)

    with pytest.raises(OSError):
        write_checkpoint(root, Checkpoint(DAY_TWO, walked.entries), recorded_at=FIRST_NIGHT)

    assert not list(checkpoint_path(root).parent.glob("*.tmp-*"))


def test_the_saved_mappings_are_sorted_whatever_the_master_order():
    master = SecurityMaster(
        [_mapping(2, "SPY", valid_from=DAY_THREE), _mapping(1, "SPY", valid_to=DAY_THREE)]
    )

    assert mappings_at(master, "SPY", DAY_THREE) == (
        (1, "ticker", date(2026, 9, 8), DAY_THREE),
        (2, "ticker", DAY_THREE, None),
    )


def test_the_saved_mappings_come_from_the_master_the_walk_started_with(
    fixture_lake: FixtureLake, monkeypatch
):
    """A master edited mid-walk then reads as changed at the next resume, which refuses."""
    root = _lake(fixture_lake, RETURNING_LAKE)
    before = SecurityMaster.read(master_path(root))
    real = splits.read_session

    def editing(lake_root, ticker, day, instrument_id):
        SecurityMaster([_mapping(1, "SPY", valid_from=date(2026, 9, 1))]).write(master_path(root))
        return real(lake_root, ticker, day, instrument_id)

    monkeypatch.setattr(splits, "read_session", editing)

    walked = walk_splits(
        lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR, edge=DAY_TWO
    )

    (entry,) = walked.entries
    assert entry.mappings == mappings_at(before, "SPY", entry.state.cutoff)


def _trim_quotes(root: Path, day: date) -> None:
    partition = f"quotes/ticker=SPY/date={day.isoformat()}.parquet"
    manifest = latest_entries(root)
    with lake_lock(root):
        line = trim_line(
            partition,
            sha256=manifest[partition]["sha256"],
            version_id="v1",
            verified_at=STAMP,
            trimmed_at=STAMP,
        )
        append_trimmed(root, line, source="trim", fetched_at=None)
        (root / partition).unlink()


def test_a_trimmed_quotes_day_is_not_a_chains_absence(fixture_lake: FixtureLake):
    """Quotes trims arrive with marketlake #794, and must not class a ticker as trimmed."""
    root = _lake(fixture_lake, RETURNING_LAKE, quotes=("SPY", DAY_ONE))
    _trim_quotes(root, DAY_ONE)

    walked = _night_two(root)

    assert walked.report.refused == ()


def test_a_quarantined_quotes_day_does_not_refuse_a_resume(fixture_lake: FixtureLake):
    root = _lake(fixture_lake, RETURNING_LAKE, quotes=("SPY", DAY_ONE))
    _night_one(root, edge=DAY_TWO)
    _trim(root, DAY_ONE)
    quotes_day = f"quotes/ticker=SPY/date={DAY_ONE.isoformat()}.parquet"
    append_quarantine(root, {"partition": quotes_day, "verdict": "bad"})

    walked = _night_two(root)

    assert walked.report.refused == ()


def test_a_refusal_with_no_saved_entry_names_its_repair(fixture_lake: FixtureLake):
    root = _lake(fixture_lake, RETURNING_LAKE)
    _trim(root, DAY_ONE)

    walked = _night_two(root)

    assert REPAIR in _refused(walked)["SPY"]


def test_a_resumed_ticker_refused_mid_walk_reports_no_state(fixture_lake: FixtureLake):
    root = _lake(fixture_lake, RETURNING_LAKE)
    saved = _night_one(root, edge=DAY_ONE)
    (root / _partition(DAY_TWO)).unlink()

    report = splits.detect_splits(
        lake_root=root,
        clock=ManualClock(SECOND_NIGHT),
        calendar=CALENDAR,
        resume=[saved.entries[0].state],
        absent_refusal=lambda ticker, day: "refused mid-walk",
    )

    assert [(r.ticker, r.reason) for r in report.refused] == [("SPY", "refused mid-walk")]
    assert report.states == ()


def test_an_unreadable_checkpoint_is_named_on_stderr(fixture_lake: FixtureLake, capsys):
    """The report file keeps the class alone. The whole message reaches the job's log."""
    root = _lake(fixture_lake, RETURNING_LAKE)
    _night_one(root, edge=DAY_ONE)
    checkpoint_path(root).write_bytes(b"PAR1 torn")

    _night_two(root)

    assert "splits: the split checkpoint at" in capsys.readouterr().err


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a directory whatever its mode")
def test_an_absence_that_cannot_be_checked_raises(fixture_lake: FixtureLake):
    """A ticker directory that cannot be read is not a designed absence."""
    root = _lake(fixture_lake, RETURNING_LAKE)
    _trim(root, DAY_ONE)
    directory = (root / _partition(DAY_ONE)).parent
    directory.chmod(0)
    try:
        with pytest.raises(PermissionError):
            _night_two(root)
    finally:
        directory.chmod(0o755)


def test_a_night_refusing_every_ticker_still_restamps_the_saved_checkpoint(
    fixture_lake: FixtureLake,
):
    """No state tonight is not a reason to drop the saved entries."""
    root = _swept_lake(fixture_lake)
    master = SecurityMaster.read(master_path(root))
    saved_day = date(2026, 9, 11)
    saved = Checkpoint(
        session_day=saved_day,
        entries=(
            CheckpointEntry(
                state=WalkState("SPY", saved_day, None, frozenset(), (), 0, saved_day),
                mappings=mappings_at(master, "SPY", saved_day),
            ),
        ),
    )
    write_checkpoint(root, saved, recorded_at=FIRST_NIGHT)
    _trim(root, SESSION)

    outcome, _ = _sweep(root)

    assert any(p.startswith("splits did not run for SPY") for p in outcome.nightly.problems)
    written = read_checkpoint(root)
    assert written.session_day == SESSION
    assert written.entry("SPY") == saved.entry("SPY")


class _RefusingBefore:
    """A calendar that answers like ``inner`` and refuses every day before ``first``.

    ``refusal`` is what it raises: a ``ValueError`` the way ``exchange_calendars`` raises
    ``DateOutOfBounds``, or some other class the edge's own refusals do not name.
    """

    def __init__(self, inner, first: date, refusal: type[Exception]) -> None:
        self._inner = inner
        self._first = first
        self._refusal = refusal

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def is_session(self, day: date) -> bool:
        if day < self._first:
            raise self._refusal(f"{day.isoformat()} is before the calendar's first session")
        return self._inner.is_session(day)


@pytest.mark.parametrize("refusal", [ValueError, ArithmeticError], ids=["ValueError", "other"])
def test_a_window_past_the_calendars_first_session_still_files_and_pings(
    fixture_lake: FixtureLake, refusal: type[Exception]
):
    """The night keeps its report and its ping, and the line says what went wrong.

    A ``ValueError`` is the shape ``exchange_calendars`` refuses with, which ``window_edge``
    turns into its own refusal. Any other class reaches the sweep's backstop.
    """
    root = _swept_lake(fixture_lake)
    calendar = _RefusingBefore(LONG_CALENDAR, date(2016, 8, 3), refusal)

    outcome, pinger = _sweep(root, window=6000, calendar=calendar)

    assert outcome.filed_at is not None
    assert pinger.events
    assert any(line.startswith("lake window edge not found: ") for line in outcome.nightly.report)
    assert not checkpoint_path(root).exists()
