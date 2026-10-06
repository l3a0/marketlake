"""Compaction's two whole-partition reads, streamed in batches so a ticker-day fits a small host.

A replay of a real two-ticker day on a 1.8 GiB host was killed by the kernel during the
first chains seal (marketlake #660). The seal decoded the whole partition it had just
written to count its rows, and the nightly re-tune decoded five columns of every chains
partition to profile them. Both reads now go through ``iter_batches``, so a read holds one
batch at a time however large the day grows.

Three rules have to survive that change, and groups 1, 2 and 4 below are about them.

1. Every page still decodes. The count is the only thing that turns a partition whose
   pages do not decode into a raise before the manifest append. A count taken from the
   footer decodes nothing, so a page that fails to decode is the case that tells the two
   apart.
2. A sealed partition still reads whole. A batched read trusts footer facts that a
   whole-table read refuses on, so the verify checks the footer against the schema that
   was written. Without that, a day the loader cannot read would seal and lose its
   segments.
3. The re-tune's profile is unchanged. A cycle's rows can straddle two batches, so each
   cycle's count has to be summed across batches before the peak per window is taken.

These cover:

1. A partition with a damaged data page, in any of three columns, raises at the verify,
   manifests nothing, and leaves the segments, the backup and the ping alone. A zero-row
   seal counts zero over zero batches, and a partition of several row groups counts
   every group.
2. A footer that renames a column, stores a chunk under the wrong physical type, or
   declares a chunk one value short raises the same way, though every page decodes and
   the count matches.
3. The verify's and the re-tune's peak Arrow memory stay under fixed bounds on a
   400,000-row day, which decoding the whole file at once exceeds by an order of
   magnitude.
4. The streamed profile equals the whole-table profile it replaced, on shuffled rows read
   in small batches, and its peak memory stays under a fixed bound. A first batch with
   no windowed rows does not end the profile, and every row group is profiled.
5. The profile's null rules, and a partition without the window columns, which the
   re-tune skips.

The memory tests install a proxy pool, which counts every byte Arrow requests through it.
Three constraints on it were measured on pyarrow 25.0.1 and are kept here.

1. The proxy is installed before the read opens the file, because a proxy installed
   afterwards counts nothing.
2. Every proxy stays referenced for the life of the test process, in ``_PROXIES``. Memory a
   threaded read allocated through a proxy can be freed later from a background thread,
   and a proxy freed before that crashed the interpreter in a later, unrelated test.
3. A reference read the test takes itself passes ``use_threads=False`` for the same reason.
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime, time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from lake import compact, journal
from lake.calendar import MARKET_TZ
from lake.chain_plan import DEFAULT_CHAIN_PLAN, Window
from lake.compact import CompactionVerifyError, WindowProfile, _write_partition, window_profile
from lake.config import GuardConstants
from lake.journal import CHAINS_SCHEMA, ROW_KIND_DATA
from lake.manifest import read_manifest
from lake.paths import LakePaths
from tests.component.test_compaction import (
    DAY,
    PID,
    _chains,
    _chains_rows,
    _iso_windows,
    _profile_table,
    _rel,
    _run,
    _segment,
    _snap,
    _table,
)
from tests.support.backup import FakeBackup
from tests.support.clock import ManualClock
from tests.support.lake import FixtureLake
from tests.support.pinger import FakePinger

# Every proxy pool a test installs, kept alive until the process exits. See the module
# docstring's second constraint for why a proxy must never be freed.
_PROXIES: list[pa.MemoryPool] = []

# The profile columns, spelled here rather than read from ``compact`` so the reference
# read below does not move with the code under test.
_PROFILE = ["ticker", "snap_ts", "row_kind", "window_start", "window_end"]


@contextmanager
def _measured() -> Iterator[pa.MemoryPool]:
    """Route every Arrow allocation through a fresh proxy pool, then restore the pool."""
    original = pa.default_memory_pool()
    proxy = pa.proxy_memory_pool(original)
    _PROXIES.append(proxy)
    pa.set_memory_pool(proxy)
    try:
        yield proxy
    finally:
        pa.set_memory_pool(original)


def _reference_profile(table: pa.Table, session_date: date) -> WindowProfile:
    """The whole-table profile ``window_profile`` computed before it streamed.

    Kept verbatim from ``lake.compact`` at 543c0b9 as the oracle the streamed version is
    compared to. It decodes the whole table at once, which is what the change removed.
    """
    if table.num_rows == 0:
        return WindowProfile({}, frozenset())
    windowed = table.filter(pc.is_valid(table.column("window_start")))
    if windowed.num_rows == 0:
        return WindowProfile({}, frozenset())
    kinds = windowed.column("row_kind")
    data = windowed.filter(pc.equal(kinds, ROW_KIND_DATA))
    gaps = windowed.filter(pc.not_equal(kinds, ROW_KIND_DATA))

    peaks: dict[Window, int] = {}
    if data.num_rows:
        per_cycle = data.group_by(["ticker", "snap_ts", "window_start", "window_end"]).aggregate(
            [([], "count_all")]
        )
        by_window = per_cycle.group_by(["window_start", "window_end"]).aggregate(
            [("count_all", "max")]
        )
        for row in by_window.to_pylist():
            peaks[compact._offsets(row, session_date)] = int(row["count_all_max"])

    failed: set[Window] = set()
    if gaps.num_rows:
        distinct = gaps.group_by(["window_start", "window_end"]).aggregate([])
        for row in distinct.to_pylist():
            failed.add(compact._offsets(row, session_date))
    return WindowProfile(peaks, frozenset(failed))


# -- 1. the verify still decodes every page ----------------------------------


def _damage_a_data_page(partition: Path, column: str) -> tuple[int, int]:
    """Overwrite the tail of ``column``'s data page in place, and return the range.

    The range is the last eight bytes of the column chunk, which sit in the data page's
    encoded values, past its header and well before the footer. Filling them with 0xFF
    makes the page decode to the wrong number of bytes, so a read that decodes the page
    raises, while the footer still parses and still states the row count.
    """
    metadata = pq.read_metadata(partition)
    names = [metadata.schema.column(index).name for index in range(metadata.num_columns)]
    chunk = metadata.row_group(0).column(names.index(column))
    start = chunk.dictionary_page_offset or chunk.data_page_offset
    end = start + chunk.total_compressed_size
    damaged = bytearray(partition.read_bytes())
    damaged[end - 8 : end] = b"\xff" * 8
    partition.write_bytes(bytes(damaged))
    return chunk.data_page_offset, end


# Three columns rather than one, so a verify that decoded only some columns fails here.
@pytest.mark.parametrize("column", ["occ_symbol", "ticker", "snap_ts"])
def test_a_partition_whose_data_page_fails_to_decode_raises_and_manifests_nothing(
    lake_root, monkeypatch, column
):
    """Rule 3's count is what notices a page that does not decode.

    The fault goes in the same seam as the wrong-count test in ``test_compaction.py``:
    ``_write_partition`` writes the real file and then damages one data page. The footer
    still parses and still says three rows, so a count read from it would match the sum
    across the segments and bless the file. Only decoding the page raises.
    """
    segment = _segment(
        lake_root, "chains", "SPY", DAY, _chains(3, snap_ts=_snap(DAY, 0)), start_ts="a"
    )
    partition = LakePaths(lake_root).chains_partition_path("SPY", DAY)
    damage: list[tuple[int, int]] = []

    def write_damaged(table: pa.Table, target: Path) -> None:
        _write_partition(table, target)
        damage.append(_damage_a_data_page(target, column))

    monkeypatch.setattr("lake.compact._write_partition", write_damaged)
    events: list[str] = []

    with pytest.raises(OSError, match="corrupt data page"):
        _run(lake_root, backup=FakeBackup(events), pinger=FakePinger(events))

    # The damage sits inside a data page and not in the footer: the footer still reads,
    # and a reference decode of the same file fails the same way.
    (page_start, page_end) = damage[0]
    assert page_start < page_end < partition.stat().st_size
    assert pq.read_metadata(partition).num_rows == 3
    with pytest.raises(OSError, match="corrupt data page"):
        pq.read_table(partition, use_threads=False)
    # Nothing blessed the damaged file, the segments are still there to rebuild from, and
    # the raise came before the backup and the ping.
    assert read_manifest(lake_root) == []
    assert segment.exists()
    assert events == []


def test_a_ticker_day_with_no_rows_seals_and_verifies_as_zero(lake_root):
    """A seal where no segment yields a table writes the surface's empty table.

    A segment of zero bytes, left by a crash between its create and its first write,
    reads as no table at all. With nothing else on the ticker-day, the seal writes the
    surface's empty table. The verify then reads a file that yields no batch, and has to
    count that as zero rather than fail on it.
    """
    empty = journal.segment_path(lake_root, "chains", "SPY", DAY, "a", PID)
    empty.parent.mkdir(parents=True, exist_ok=True)
    empty.write_bytes(b"")
    partition = LakePaths(lake_root).chains_partition_path("SPY", DAY)

    result, events, _, _ = _run(lake_root)

    assert [item.rows for item in result.sealed] == [0]
    assert pq.read_metadata(partition).num_rows == 0
    assert [entry["rows"] for entry in read_manifest(lake_root)] == [0]
    assert not empty.exists()
    assert events


def test_a_partition_of_several_row_groups_counts_every_group(lake_root, monkeypatch):
    """The verify counts every row group, not only the first.

    Every other fixture here seals one row group, so a count that read only the first
    group would pass them all. The writer seam writes five rows in groups of two, which is
    three groups, and the seal has to count all five to match the segments.
    """
    segment = _segment(
        lake_root, "chains", "SPY", DAY, _chains(5, snap_ts=_snap(DAY, 0)), start_ts="a"
    )
    partition = LakePaths(lake_root).chains_partition_path("SPY", DAY)

    def write_in_small_groups(table: pa.Table, target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, target, row_group_size=2)

    monkeypatch.setattr("lake.compact._write_partition", write_in_small_groups)

    result, events, _, _ = _run(lake_root)

    assert pq.read_metadata(partition).num_row_groups == 3
    assert [item.rows for item in result.sealed] == [5]
    assert [entry["rows"] for entry in read_manifest(lake_root)] == [5]
    assert not segment.exists()
    assert events


# -- 2. the verify checks the footer a whole read trusts ---------------------


def _footer_range(data: bytes) -> tuple[int, int]:
    """Where a Parquet file's footer starts and ends, from the length in its last 8 bytes."""
    end = len(data) - 8
    return end - int.from_bytes(data[end : end + 4], "little"), end


def _edit_footer(path: Path, find: bytes, edit, wanted) -> None:
    """Rewrite one site in ``path``'s footer, chosen by what the edited footer then says.

    The footer is Thrift's compact encoding, so the bytes for one field can occur in many
    places. Each occurrence of ``find`` is edited in turn with ``edit``, and the first one
    whose footer still parses and satisfies ``wanted`` is written back. Searching the
    sites rather than fixing an offset keeps the fixture right when the footer's layout
    shifts with the schema.
    """
    good = path.read_bytes()
    start, end = _footer_range(good)
    site = good.find(find, start, end)
    while site >= 0:
        edited = bytearray(good)
        edit(edited, site)
        try:
            metadata = pq.read_metadata(pa.BufferReader(bytes(edited)))
        except OSError:
            metadata = None
        if metadata is not None and wanted(metadata):
            path.write_bytes(bytes(edited))
            return
        site = good.find(find, site + 1, end)
    raise AssertionError(f"no site in the footer of {path} gives the edit wanted")


def _chunk_types_that_disagree(metadata: pq.FileMetaData) -> list[tuple[int, str]]:
    """Each ``(row group, column)`` whose chunk's physical type is not the schema's."""
    return [
        (group, metadata.schema.column(index).name)
        for group in range(metadata.num_row_groups)
        for index in range(metadata.num_columns)
        if metadata.row_group(group).column(index).physical_type
        != metadata.schema.column(index).physical_type
    ]


def _rename_occ_symbol(target: Path) -> None:
    """Rename ``occ_symbol`` to ``occ_symbox`` in the footer, in both places it is named.

    A name of the same length keeps every other footer byte where it was, so the footer
    still parses and every page still decodes under the new name.
    """
    data = target.read_bytes()
    start, end = _footer_range(data)
    footer = data[start:end]
    assert footer.count(b"occ_symbol") == 2
    target.write_bytes(data[:start] + footer.replace(b"occ_symbol", b"occ_symbox") + data[end:])


def _store_bid_as_float_in_the_last_group(target: Path) -> None:
    """Relabel ``bid``'s chunk in the last of three row groups from DOUBLE to FLOAT.

    The compact encoding writes a chunk's type as the field header 0x15 and then the type
    doubled, so DOUBLE, which is 5, is the byte 0x0A and FLOAT, which is 4, is 0x08. The
    schema still says DOUBLE, so only the chunk disagrees.
    """

    def relabel(edited: bytearray, site: int) -> None:
        edited[site + 1] = 0x08

    _edit_footer(
        target,
        b"\x15\x0a",
        relabel,
        lambda metadata: _chunk_types_that_disagree(metadata) == [(2, "bid")],
    )


def _declare_bid_one_value_short(target: Path) -> None:
    """Make the footer declare two values in ``bid``'s chunk for its three rows.

    The compact encoding writes a count as its zigzag varint, so 3 is the byte 6 and 2 is
    the byte 4. Every other chunk keeps three values and the row group keeps three rows.
    """

    def decrement(edited: bytearray, site: int) -> None:
        edited[site] = 4

    def only_bid_is_short(metadata: pq.FileMetaData) -> bool:
        group = metadata.row_group(0)
        values = {
            metadata.schema.column(index).name: group.column(index).num_values
            for index in range(metadata.num_columns)
        }
        return group.num_rows == 3 and values.pop("bid") == 2 and set(values.values()) == {3}

    _edit_footer(target, b"\x06", decrement, only_bid_is_short)


# Each footer rewrite, and the row group size the file is written at before it.
_FOOTER_REWRITES = {
    "renamed-column": (_rename_occ_symbol, None),
    "chunk-physical-type": (_store_bid_as_float_in_the_last_group, 1),
    "chunk-value-count": (_declare_bid_one_value_short, None),
}


@pytest.mark.parametrize(
    ("case", "said"),
    [
        ("renamed-column", "column 5 occ_symbox (string) where occ_symbol (string) was written"),
        ("chunk-physical-type", "row group 2 stores bid as FLOAT, but the schema says DOUBLE"),
        ("chunk-value-count", "row group 0 declares 2 values in bid for 3 rows"),
    ],
)
def test_a_footer_that_disagrees_with_the_write_raises_and_manifests_nothing(
    lake_root, monkeypatch, case, said
):
    """The verify refuses a footer that disagrees with what was written.

    Each case rewrites one fact in the footer the seal just wrote, through the same writer
    seam as the damaged-page test. In every case each page still decodes and the batched
    count still matches the segments, which the next test shows, so the count alone would
    seal the file and unlink the segments.

    1. A renamed column reads under the new name, so a reader asking for ``occ_symbol``
       finds nothing.
    2. A chunk stored under a physical type the schema does not name is refused by a
       whole-table read.
    3. A chunk declaring one value short of its row group is refused by a whole-table
       read, the loader's included.

    The chunk-type case writes three row groups, because a batched read of a single group
    refuses that chunk on its own.
    """
    rewrite, row_group_size = _FOOTER_REWRITES[case]
    segment = _segment(
        lake_root, "chains", "SPY", DAY, _chains(3, snap_ts=_snap(DAY, 0)), start_ts="a"
    )

    def write_rewritten(table: pa.Table, target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, target, row_group_size=row_group_size)
        rewrite(target)

    monkeypatch.setattr("lake.compact._write_partition", write_rewritten)
    events: list[str] = []

    with pytest.raises(CompactionVerifyError) as info:
        _run(lake_root, backup=FakeBackup(events), pinger=FakePinger(events))

    # The count matched, so the footer check is what refused.
    assert (info.value.expected, info.value.actual) == (3, 3)
    assert info.value.disagreement is not None
    assert said in info.value.disagreement
    assert str(info.value).endswith(f"re-read 3, but {info.value.disagreement}")
    assert read_manifest(lake_root) == []
    assert segment.exists()
    assert events == []


@pytest.mark.parametrize("case", list(_FOOTER_REWRITES))
def test_each_footer_rewrite_passes_a_batched_count(tmp_path, case):
    """Each footer rewrite above is one a batched count alone would seal.

    Without this, a rewrite that also broke a page would pass the footer test for the
    wrong reason, because the count would raise first. A batched read of each rewritten
    file returns all three rows. A whole-table read refuses the chunk-type and value-count
    files, and reads the renamed file without ``occ_symbol``.
    """
    rewrite, row_group_size = _FOOTER_REWRITES[case]
    path = tmp_path / "chains.parquet"
    pq.write_table(_chains(3, snap_ts=_snap(DAY, 0)), path, row_group_size=row_group_size)
    rewrite(path)

    batches = pq.ParquetFile(path).iter_batches(batch_size=8192, use_threads=False)
    assert sum(batch.num_rows for batch in batches) == 3
    if case == "renamed-column":
        assert "occ_symbol" not in pq.read_table(path, use_threads=False).column_names
    elif case == "chunk-physical-type":
        with pytest.raises(OSError, match="ColumnMetaData type does not match"):
            pq.read_table(path, use_threads=False)
    else:
        with pytest.raises(pa.ArrowInvalid, match="named bid expected length 3 but got length 2"):
            pq.read_table(path, use_threads=False)


# -- 3. the verify and the re-tune hold one batch at a time ------------------


def test_the_verify_reads_a_large_day_in_bounded_memory(lake_root):
    """The seal's peak Arrow memory stays far under the size of the decoded day.

    One 10,000-row cycle repeated forty times is a 400,000-row ticker-day in one segment.
    The merge reads the segment through a memory map and the writer encodes one row group
    at a time, so the verify's decode is what sets the peak. Decoding the whole file at
    once peaked at 222 to 240 MB on this fixture, and 8,192-row batches at about 12 MB.
    The peak grows with the batch size, to about 48 MB at 32,768 rows, so 24 MB fails
    both a whole-file read and a batch four times the size.
    """
    counts = {0: 2000, 1: 2000, 2: 2000, 3: 2000, 4: 2000}
    cycle = _profile_table(DEFAULT_CHAIN_PLAN, DAY, counts, snap_ts=_snap(DAY, 0))
    day_table = pa.concat_tables([cycle] * 40)
    segment = _segment(lake_root, "chains", "SPY", DAY, day_table, start_ts="a")
    paths = LakePaths(lake_root)
    clock = ManualClock(datetime.combine(DAY, time(16, 30), tzinfo=MARKET_TZ))

    with _measured() as pool:
        sealed = compact._seal(
            lake_root, paths, "chains", "SPY", DAY, [segment], clock=clock, guard=False, entries={}
        )

    assert sealed.rows == 400_000
    assert pool.max_memory() < 24_000_000


def test_the_retune_reads_a_large_day_in_bounded_memory(lake_root):
    """The re-tune's peak Arrow memory stays far under the size of the decoded columns.

    This measures the call production makes, ``_retune`` with no batch size of its own,
    on the same 400,000-row day as the verify test above. The day is sealed first, outside
    the measurement, so only the profile is counted. Its streamed read peaked at about
    2.7 MB. A batch of 32,768 rows peaked at about 10.4 MB, and one batch holding the
    whole day at about 91.8 MB, so 6 MB fails both.
    """
    counts = {0: 2000, 1: 2000, 2: 2000, 3: 2000, 4: 2000}
    cycle = _profile_table(DEFAULT_CHAIN_PLAN, DAY, counts, snap_ts=_snap(DAY, 0))
    segment = _segment(
        lake_root, "chains", "SPY", DAY, pa.concat_tables([cycle] * 40), start_ts="a"
    )
    paths = LakePaths(lake_root)
    clock = ManualClock(datetime.combine(DAY, time(16, 30), tzinfo=MARKET_TZ))
    sealed = compact._seal(
        lake_root, paths, "chains", "SPY", DAY, [segment], clock=clock, guard=False, entries={}
    )

    with _measured() as pool:
        retune = compact._retune(
            lake_root,
            [sealed],
            DAY,
            guards=GuardConstants(),
            plan_path=lake_root.parent / "chain_plan.json",
        )

    assert retune.counts == (80_000, 80_000, 80_000, 80_000, 80_000)
    assert pool.max_memory() < 6_000_000


# -- 4. the streamed profile -------------------------------------------------


def _windowed_rows(cycles: list[tuple[str, int, dict[int, int]]]) -> list[dict]:
    """Data rows for each ``(ticker, minute, counts)`` cycle, in the default plan's windows."""
    windows = _iso_windows(DEFAULT_CHAIN_PLAN, DAY)
    rows: list[dict] = []
    for ticker, minute, counts in cycles:
        for index, count in counts.items():
            rows += _chains_rows(
                count, snap_ts=_snap(DAY, minute), ticker=ticker, window=windows[index]
            )
    return rows


def _shuffled_day(path: Path) -> Path:
    """A chains partition of four cycles over two tickers, in deliberately shuffled order.

    Compaction writes a cycle's rows together, so a cycle straddles at most one batch
    boundary and only when it is large. Shuffling spreads every cycle over every batch,
    so a profile that took the largest piece of a cycle instead of the sum of its pieces
    answers far short. Beside the data rows sit gap rows in two windows, rows with no
    window, and rows with no ``row_kind``.
    """
    windows = _iso_windows(DEFAULT_CHAIN_PLAN, DAY)
    rows = _windowed_rows(
        [
            ("SPY", 0, {0: 1500, 1: 400, 2: 300, 3: 200, 4: 900}),
            ("SPY", 1, {0: 1700, 1: 350, 4: 950}),
            ("SPY", 2, {0: 1200, 1: 500, 3: 250, 4: 800}),
            ("QQQ", 0, {0: 1600, 2: 450, 4: 300}),
        ]
    )
    rows += _chains_rows(3, snap_ts=_snap(DAY, 1), window=windows[2], row_kind="gap")
    rows += _chains_rows(2, snap_ts=_snap(DAY, 2), window=windows[3], row_kind="gap")
    rows += _chains_rows(100, snap_ts=_snap(DAY, 0))
    rows += _chains_rows(5, snap_ts=_snap(DAY, 1), window=windows[1], row_kind=None)
    random.Random(7).shuffle(rows)
    pq.write_table(_table(CHAINS_SCHEMA, rows), path)
    return path


# The shuffled day's peak per window, worked out by hand from the cycles above.
_SHUFFLED_PEAKS = {(0, 9): 1700, (10, 30): 500, (31, 90): 450, (91, 365): 250, (366, None): 950}
_SHUFFLED_FAILED = frozenset({(31, 90), (91, 365)})


@pytest.mark.parametrize("batch_size", [64, 256, 1024, 100_000])
def test_the_streamed_profile_equals_the_whole_table_profile_on_shuffled_rows(tmp_path, batch_size):
    path = _shuffled_day(tmp_path / "chains.parquet")
    reference = _reference_profile(pq.read_table(path, columns=_PROFILE, use_threads=False), DAY)

    streamed = window_profile(path, DAY, batch_size=batch_size)

    assert streamed == reference
    assert dict(streamed.peaks) == _SHUFFLED_PEAKS
    assert streamed.failed == _SHUFFLED_FAILED


def test_the_streamed_profile_reads_in_bounded_memory(tmp_path):
    """The profile holds one batch of five columns, never the partition's columns whole.

    On this fixture's 11,510 shuffled rows read 256 at a time, the streamed profile
    peaked at about 0.1 MB. Reading the five columns whole peaked at about 2.4 MB, and so
    did one batch large enough to hold the whole file.
    """
    path = _shuffled_day(tmp_path / "chains.parquet")

    with _measured() as pool:
        profile = window_profile(path, DAY, batch_size=256)

    assert dict(profile.peaks) == _SHUFFLED_PEAKS
    assert pool.max_memory() < 500_000


def test_a_first_batch_with_no_windowed_rows_does_not_end_the_profile(tmp_path):
    """A batch with no windowed rows is skipped, and the batches after it still count.

    The first 9,000 rows carry no window, so at 1,000 rows a batch the first nine batches
    hold nothing to profile. The windowed rows come after them, and a profile that stopped
    at the first empty batch would return nothing.
    """
    windows = _iso_windows(DEFAULT_CHAIN_PLAN, DAY)
    rows = _chains_rows(9000, snap_ts=_snap(DAY, 0))
    rows += _chains_rows(1200, snap_ts=_snap(DAY, 0), window=windows[0])
    rows += _chains_rows(300, snap_ts=_snap(DAY, 0), window=windows[1])
    rows += _chains_rows(2, snap_ts=_snap(DAY, 0), window=windows[2], row_kind="gap")
    path = tmp_path / "chains.parquet"
    pq.write_table(_table(CHAINS_SCHEMA, rows), path)

    profile = window_profile(path, DAY, batch_size=1000)

    assert dict(profile.peaks) == {(0, 9): 1200, (10, 30): 300}
    assert profile.failed == frozenset({(31, 90)})


def test_the_profile_reads_every_row_group(tmp_path):
    """Rows in later row groups count as much as rows in the first.

    Cycle 0 fills the first of five 100-row groups, and cycle 1, the larger, fills the
    next three. The gap rows sit in the last group. A profile of the first group alone
    would report cycle 0's 100 contracts and no failed window.
    """
    windows = _iso_windows(DEFAULT_CHAIN_PLAN, DAY)
    rows = _chains_rows(100, snap_ts=_snap(DAY, 0), window=windows[0])
    rows += _chains_rows(300, snap_ts=_snap(DAY, 1), window=windows[0])
    rows += _chains_rows(2, snap_ts=_snap(DAY, 1), window=windows[2], row_kind="gap")
    path = tmp_path / "chains.parquet"
    pq.write_table(_table(CHAINS_SCHEMA, rows), path, row_group_size=100)

    profile = window_profile(path, DAY)

    assert pq.read_metadata(path).num_row_groups == 5
    assert dict(profile.peaks) == {(0, 9): 300}
    assert profile.failed == frozenset({(31, 90)})


# -- 5. the null rules, and a partition with no windows ----------------------


def test_the_profile_counts_a_null_row_kind_as_neither_data_nor_gap(tmp_path):
    """Three rules decide what a row with a null in the profile columns counts as.

    A null ``window_start`` leaves the row out. A null ``window_end`` is the open tail,
    which counts as its own window. A null ``row_kind`` counts as neither data nor gap,
    so it neither raises a window's count nor marks it failed. ``(0, 9)`` carries data
    and null-kind rows, and ``(10, 30)`` carries only null-kind rows, so a null read as
    data raises the first and adds the second, and a null read as a gap fails both.
    """
    windows = _iso_windows(DEFAULT_CHAIN_PLAN, DAY)
    rows = _chains_rows(5, snap_ts=_snap(DAY, 0), window=windows[0])
    rows += _chains_rows(7, snap_ts=_snap(DAY, 0), window=windows[0], row_kind=None)
    rows += _chains_rows(4, snap_ts=_snap(DAY, 0), window=windows[1], row_kind=None)
    rows += _chains_rows(6, snap_ts=_snap(DAY, 0), window=windows[4])
    rows += _chains_rows(9, snap_ts=_snap(DAY, 0), window=(None, windows[0][1]))
    path = tmp_path / "chains.parquet"
    pq.write_table(_table(CHAINS_SCHEMA, rows), path)

    profile = window_profile(path, DAY, batch_size=4)

    assert windows[4][1] is None
    assert dict(profile.peaks) == {(0, 9): 5, (366, None): 6}
    assert profile.failed == frozenset()
    reference = _reference_profile(pq.read_table(path, columns=_PROFILE, use_threads=False), DAY)
    assert profile == reference


def test_a_partition_without_the_window_columns_is_left_out_of_the_retune(lake_root):
    """A chains partition from before the windowed fetch has nothing to say about the plan.

    SPY's partition is already sealed in the older shape, with no ``window_start`` or
    ``window_end``, so the run recovers it and the re-tune meets it. QQQ seals from a
    segment. The re-tune profiles QQQ alone rather than failing on SPY's missing columns.
    """
    plan_path = lake_root.parent / "chain_plan.json"
    older = _chains(3, snap_ts=_snap(DAY, 0)).drop_columns(["window_start", "window_end"])
    FixtureLake(lake_root).with_partition("chains", "SPY", DAY, older).build()
    _segment(lake_root, "chains", "SPY", DAY, _chains(1, snap_ts=_snap(DAY, 5)), start_ts="a")
    steady = {0: 1000, 1: 1000, 2: 1000, 3: 1000, 4: 1000}
    qqq = _profile_table(DEFAULT_CHAIN_PLAN, DAY, steady, snap_ts=_snap(DAY, 0), ticker="QQQ")
    _segment(lake_root, "chains", "QQQ", DAY, qqq, start_ts="a")

    result, _, _, _ = _run(lake_root, plan_path=plan_path)

    spy = LakePaths(lake_root).chains_partition_path("SPY", DAY)
    assert "window_start" not in pq.read_schema(spy).names
    assert [item.recovered for item in result.verified] == [True]
    assert {_rel(lake_root, lake_root / item.partition) for item in result.sealed} == {
        _rel(lake_root, LakePaths(lake_root).chains_partition_path("QQQ", DAY))
    }
    assert result.retune.skipped_reason is None
    assert result.retune.counts == (1000, 1000, 1000, 1000, 1000)
    assert not result.retune.written
