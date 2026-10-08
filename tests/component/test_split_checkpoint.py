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

import os
import shutil
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import actions, splits, sweep
from lake.alert import Publisher
from lake.lock import lake_lock
from lake.manifest import (
    ManifestError,
    append_quarantine,
    latest_entries,
    record_partition,
    sha256_file,
)
from lake.paths import CHAINS, TRIMMED_FILE, LakePaths
from lake.report import SPLITS_PIECE
from lake.security_master import SecurityMaster, master_path
from lake.split_checkpoint import (
    CHECKPOINT_PARTITION,
    Checkpoint,
    CheckpointEntry,
    CheckpointUnreadable,
    checkpoint_path,
    mappings_at,
    read_checkpoint,
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
