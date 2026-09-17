"""The manifest ledger across one real boundary: the filesystem.

These write real ``O_APPEND`` lines, checksum real files, and run the two-way scrub
over a lake the fixture builder put on disk. The clock stays out of it. ``fetched_at``
is passed in.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import UTC, date, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake.manifest import (
    LedgerNotUtf8,
    ManifestError,
    RowCountRegression,
    TornLedger,
    append_line,
    append_manifest,
    append_quarantine,
    guard_row_count,
    latest_entries,
    latest_quarantine,
    latest_quarantine_by_check,
    manifest_path,
    quarantine_path,
    read_manifest,
    read_quarantine,
    record_partition,
    scrub,
    sha256_file,
    would_shrink,
)
from lake.metadata import metadata_path, stamp_cycle
from lake.paths import REPORTS_DIR
from lake.tickers import Roster
from tests.support.lake import FixtureLake, sample_chains_table

DAY = date(2026, 8, 24)
CHAINS_REL = "chains/ticker=SPY/date=2026-08-24.parquet"
QUOTES_REL = "quotes/ticker=SPY/date=2026-08-24.parquet"


def _base_lake(fixture_lake: FixtureLake) -> FixtureLake:
    fixture_lake.with_chains("SPY", DAY)
    fixture_lake.with_quotes("SPY", DAY)
    fixture_lake.build()
    return fixture_lake


# -- atomic single-line append -----------------------------------------------


def test_append_writes_exactly_one_line_per_entry(lake_root):
    append_manifest(
        lake_root, partition="a", source="capture", sha256="s1", rows=1, fetched_at=None
    )
    append_manifest(
        lake_root, partition="b", source="capture", sha256="s2", rows=2, fetched_at=None
    )
    lines = manifest_path(lake_root).read_text().splitlines()
    assert len(lines) == 2
    assert [json.loads(line)["partition"] for line in lines] == ["a", "b"]
    assert read_manifest(lake_root)[1]["sha256"] == "s2"


def test_read_discards_a_real_torn_trailing_line(lake_root):
    append_manifest(
        lake_root, partition="a", source="capture", sha256="s1", rows=1, fetched_at=None
    )
    append_manifest(
        lake_root, partition="b", source="capture", sha256="s2", rows=2, fetched_at=None
    )
    # Simulate a crash mid-append: a partial line with no terminating newline.
    fd = os.open(manifest_path(lake_root), os.O_WRONLY | os.O_APPEND)
    try:
        os.write(fd, b'{"partition": "c", "sha256": "unter')
    finally:
        os.close(fd)
    entries = read_manifest(lake_root)
    assert [e["partition"] for e in entries] == ["a", "b"]


def test_concurrent_appends_never_interleave(lake_root):
    # O_APPEND makes each line atomic, so many threads appending at once still yield
    # whole, parseable lines and the exact expected count.
    per_thread = 50
    threads_n = 8

    def worker(tag: int) -> None:
        for i in range(per_thread):
            append_manifest(
                lake_root,
                partition=f"p-{tag}-{i}",
                source="capture",
                sha256="s",
                rows=1,
                fetched_at=None,
            )

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(threads_n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    lines = manifest_path(lake_root).read_text().splitlines()
    assert len(lines) == threads_n * per_thread
    partitions = {json.loads(line)["partition"] for line in lines}
    assert len(partitions) == threads_n * per_thread


def test_last_entry_wins_over_a_real_manifest(lake_root):
    append_manifest(
        lake_root, partition="p", source="capture", sha256="old", rows=100, fetched_at=None
    )
    append_manifest(
        lake_root,
        partition="p",
        source="recompaction",
        sha256="new",
        rows=405,
        fetched_at="2026-08-24T16:30:00-04:00",
        guard=False,
    )
    latest = latest_entries(lake_root)
    assert latest["p"]["sha256"] == "new"
    assert latest["p"]["rows"] == 405
    assert latest["p"]["source"] == "recompaction"


# -- real sha256 over real files ---------------------------------------------


def test_sha256_file_matches_hashlib(lake_root):
    import hashlib

    target = lake_root / "blob.bin"
    target.write_bytes(b"marketlake")
    assert sha256_file(target) == hashlib.sha256(b"marketlake").hexdigest()


def test_record_partition_checksums_the_file_on_disk(fixture_lake):
    lake = fixture_lake
    lake.build()  # empty manifest
    # Write a partition the builder did not record, then record it from disk.
    path = lake.partition_path("chains", "SPY", DAY)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(sample_chains_table(), path)
    entry = record_partition(lake.root, CHAINS_REL, source="compaction", rows=1, fetched_at=None)
    assert entry["sha256"] == sha256_file(path)
    assert scrub(lake.root).ok


# -- the standing row-count invariant ----------------------------------------


def test_guard_refuses_a_shrinking_row_count(lake_root):
    append_manifest(
        lake_root, partition="p", source="capture", sha256="s", rows=405, fetched_at=None
    )
    assert would_shrink(lake_root, "p", 400)
    assert not would_shrink(lake_root, "p", 410)
    with pytest.raises(RowCountRegression):
        guard_row_count(lake_root, "p", 400)
    # A larger or equal count is allowed.
    guard_row_count(lake_root, "p", 405)
    guard_row_count(lake_root, "p", 500)


def test_append_enforces_the_invariant_by_default(lake_root):
    append_manifest(
        lake_root, partition="p", source="capture", sha256="s", rows=405, fetched_at=None
    )
    with pytest.raises(RowCountRegression):
        append_manifest(
            lake_root, partition="p", source="capture", sha256="s2", rows=1, fetched_at=None
        )
    # A deliberate override under human authority may supersede without the guard.
    append_manifest(
        lake_root,
        partition="p",
        source="human",
        sha256="s3",
        rows=1,
        fetched_at=None,
        guard=False,
    )
    assert latest_entries(lake_root)["p"]["rows"] == 1


# -- the two-way scrub -------------------------------------------------------


def test_clean_lake_scrubs_ok(fixture_lake):
    lake = _base_lake(fixture_lake)
    assert scrub(lake.root).ok


def test_forward_pass_catches_a_sha_mismatch(fixture_lake):
    lake = _base_lake(fixture_lake)
    # Corrupt a sealed partition without touching its manifest entry.
    target = lake.partition_path("chains", "SPY", DAY)
    target.write_bytes(target.read_bytes() + b"corruption")
    result = scrub(lake.root)
    assert result.sha_mismatches == (CHAINS_REL,)
    assert not result.missing
    assert not result.orphans


def test_forward_pass_catches_a_missing_file(fixture_lake):
    lake = _base_lake(fixture_lake)
    lake.partition_path("quotes", "SPY", DAY).unlink()
    result = scrub(lake.root)
    assert result.missing == (QUOTES_REL,)


def test_reverse_pass_catches_an_orphan(fixture_lake):
    lake = _base_lake(fixture_lake)
    # A journal-less surface file with no manifest entry: exactly the invisible orphan
    # the reverse pass exists to catch.
    orphan = lake.root / "bars" / "ticker=SPY" / "freq=1m" / "date=2026-08-24.parquet"
    orphan.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"x": [1]}), orphan)
    result = scrub(lake.root)
    assert result.orphans == ("bars/ticker=SPY/freq=1m/date=2026-08-24.parquet",)
    assert not result.missing


def test_exclusion_set_is_honored(fixture_lake):
    lake = fixture_lake
    lake.with_chains("SPY", DAY)
    # A journal segment (manifest-less by rule) and the manifest itself must not read
    # as orphans.
    lake.with_journal_segment(
        "chains", "SPY", DAY, sample_chains_table(), start_ts="20260824T160000", pid=4242
    )
    lake.build()
    assert manifest_path(lake.root).exists()
    assert lake.segment_path("chains", "SPY", DAY, "20260824T160000", 4242).exists()
    assert scrub(lake.root).ok


def test_the_reports_directory_is_not_an_orphan(fixture_lake):
    # The reverse pass asks every data file for a manifest entry. The nightly report is
    # not data and never gets one. Before this the scrub called it an orphan, so the
    # first report D16 wrote would have failed the Sunday scrub and withheld its ping.
    lake = fixture_lake
    lake.with_chains("SPY", DAY)
    root = lake.build()
    reports = root / REPORTS_DIR
    reports.mkdir()
    (reports / "2026-08-24.md").write_text("Nightly 2026-08-24\n")
    result = scrub(root)
    assert result.orphans == ()
    assert result.ok


def test_the_journal_metadata_stamp_is_not_an_orphan(fixture_lake):
    # The daemon rewrites the stamp every minute, so it can carry no manifest entry. It
    # is placed under ``journal/`` for exactly that reason, inside the exclusion the
    # segments already have. Anywhere else and the Sunday scrub would call it an orphan
    # and withhold its ping every week.
    lake = fixture_lake
    lake.with_chains("SPY", DAY)
    root = lake.build()
    stamp_cycle(
        root,
        at=datetime(2026, 8, 24, 16, 0, tzinfo=UTC),
        token_minted_at=datetime(2026, 8, 23, 20, 5, tzinfo=UTC),
        roster=Roster.from_mapping({"SPY": {"options": True, "chain_cadence": "1m"}}),
    )
    assert metadata_path(root).exists()
    result = scrub(root)
    assert result.orphans == ()
    assert result.ok


def test_a_stray_file_outside_the_excluded_names_is_still_an_orphan(fixture_lake):
    # The exclusions are named, not implied. A directory that merely looks like the
    # reports one is still swept.
    lake = fixture_lake
    lake.with_chains("SPY", DAY)
    root = lake.build()
    stray = root / "reports-old"
    stray.mkdir()
    (stray / "2026-08-24.md").write_text("moved aside by hand\n")
    assert scrub(root).orphans == ("reports-old/2026-08-24.md",)


def test_slice1_segment_entry_is_a_failure_until_the_partition_is_compacted(fixture_lake):
    lake = fixture_lake
    lake.build()  # empty manifest
    seg_rel = (
        lake.segment_path("chains", "SPY", DAY, "20260824T160000", 4242)
        .relative_to(lake.root)
        .as_posix()
    )
    # A slice-1 cron entry keyed by a segment path whose file is gone, and no compacted
    # partition yet. The forward pass flags it.
    append_manifest(
        lake.root,
        partition=seg_rel,
        source="capture",
        sha256="deadbeef",
        rows=100,
        fetched_at=None,
    )
    assert scrub(lake.root).missing == (seg_rel,)


def test_slice1_segment_entry_is_superseded_by_its_compacted_partition(fixture_lake):
    lake = fixture_lake
    lake.with_chains("SPY", DAY)  # the compacted partition and its manifest entry
    lake.build()
    seg_rel = (
        lake.segment_path("chains", "SPY", DAY, "20260824T160000", 4242)
        .relative_to(lake.root)
        .as_posix()
    )
    # The segment entry lingers after compaction deleted its file. Because the compacted
    # partition now has an entry, the segment entry is superseded, not a failure.
    append_manifest(
        lake.root,
        partition=seg_rel,
        source="capture",
        sha256="deadbeef",
        rows=100,
        fetched_at=None,
    )
    assert scrub(lake.root).ok


# -- the quarantine ledger ---------------------------------------------------


def test_quarantine_appends_and_last_verdict_wins(lake_root):
    """Two entries naming no check resolve in one bucket, so the later verdict wins."""
    append_quarantine(lake_root, {"partition": CHAINS_REL, "verdict": "suspect"})
    append_quarantine(lake_root, {"partition": CHAINS_REL, "verdict": "clean"})
    assert len(read_quarantine(lake_root)) == 2
    assert quarantine_path(lake_root).read_text().splitlines()[0]
    assert latest_quarantine(lake_root)[CHAINS_REL]["verdict"] == "clean"


# -- a torn quarantine ledger refuses rather than reading short ---------------


def _verdict(partition: str) -> dict:
    return {"partition": partition, "verdict": "quarantined", "check": "entitlement"}


def _torn_after(lake_root, *behind: str) -> None:
    """One good verdict, then a write that crashed mid-line, then ``behind`` appended onto it.

    The fragment carries no terminating newline, so the first name in ``behind`` fuses onto
    it and becomes part of the same line. Every name after that one is a whole line the read
    never reaches. Built by writing the fragment directly, because no writer in the tree can
    be made to crash on demand.
    """
    append_quarantine(lake_root, _verdict("kept"))
    with quarantine_path(lake_root).open("a") as handle:
        handle.write('{"partition": "torn", "verd')
    for partition in behind:
        append_line(quarantine_path(lake_root), _verdict(partition))


def test_a_read_stopping_with_verdicts_behind_it_refuses_and_names_them(lake_root):
    """The defect marketlake #469 is. Read short, the ledger withholds nothing at all.

    Two appends land behind the fragment: the first fuses onto it and the second is hidden
    outright. Returning the entries in front of the damage would hand every consumer a
    ledger missing its own contents, and ``load_chain`` would then admit exactly the
    partitions those verdicts exist to exclude.
    """
    _torn_after(lake_root, "fused", "hidden")

    with pytest.raises(TornLedger) as refusal:
        read_quarantine(lake_root)

    assert str(quarantine_path(lake_root)) in str(refusal.value)
    assert "1 line after it is written and unreachable" in str(refusal.value)


def test_every_quarantine_reader_funnels_through_the_one_refusal(lake_root):
    """Neither resolver keeps a path around the guard.

    ``latest_quarantine`` and ``latest_quarantine_by_check`` are what ``loader``, ``sweep``,
    ``dashboard`` and ``signoff`` actually call. A refusal only ``read_quarantine`` made
    would leave all four reading short.
    """
    _torn_after(lake_root, "fused", "hidden")

    for reader in (read_quarantine, latest_quarantine, latest_quarantine_by_check):
        with pytest.raises(TornLedger):
            reader(lake_root)


def test_a_torn_tail_still_reads_because_it_hides_nothing(lake_root):
    _torn_after(lake_root)

    assert [e["partition"] for e in read_quarantine(lake_root)] == ["kept"]


def test_a_fusion_reads_until_the_next_write_lands_behind_it(lake_root):
    """``append_line``'s accepted cost, and the exact moment it stops being only that.

    The fused line costs the one entry appended onto it and hides nothing else, so refusing
    at this point would refuse the case the module already decided to pay for. Nothing
    repairs an append-only file, though, so the very next verdict lands behind a line no
    read gets past. That is where this starts refusing, and it never stops until a human
    repairs the file. Two verdicts lost, then it holds.
    """
    _torn_after(lake_root, "fused")
    assert [e["partition"] for e in read_quarantine(lake_root)] == ["kept"]

    append_line(quarantine_path(lake_root), _verdict("tonight"))
    with pytest.raises(TornLedger):
        read_quarantine(lake_root)


# -- a ledger whose bytes are not UTF-8 refuses rather than escaping ----------


def _flip(lake_root, needle: bytes, replacement: bytes) -> None:
    """Change one byte of an already written ledger, the way bit rot or a hand edit does."""
    path = quarantine_path(lake_root)
    raw = path.read_bytes()
    assert raw.count(needle) == 1, "the fixture no longer says what it meant to"
    path.write_bytes(raw.replace(needle, replacement))


def test_a_ledger_that_is_not_utf8_refuses_as_a_manifest_error(lake_root):
    """The defect marketlake #495 is.

    ``read_text`` decodes strictly, so ``UnicodeDecodeError`` came out of here. It is a
    ``ValueError`` and so neither a ``ManifestError`` nor an ``OSError``, which are the two
    families every containment around this ledger names. Escaping them cost the whole 18:30
    run: no record filed, no report, no ping, and on a Friday no Sunday wake.

    The class is what matters more than the name. Every consumer already says in writing that
    a damaged quarantine ledger raises ``ManifestError``, so refusing as one needs no tuple
    anywhere to be widened.
    """
    append_quarantine(lake_root, _verdict("kept"))
    _flip(lake_root, b'"quarantined"', b'"quarantin\xffd"')

    with pytest.raises(LedgerNotUtf8) as refusal:
        read_quarantine(lake_root)

    assert isinstance(refusal.value, ManifestError)
    assert str(quarantine_path(lake_root)) in str(refusal.value)


def test_the_refusal_names_the_byte_and_the_line_a_repair_has_to_find(lake_root):
    """The number's whole job is to send the person repairing the file to the right place.

    ``UnicodeDecodeError`` carries a byte offset alone, which is the wrong unit for an editor,
    so the line is counted from the newlines in front of it. Three whole verdicts land first,
    so a line number taken from the entries rather than the bytes would read 1 here.
    """
    for name in ("first", "second", "third"):
        append_quarantine(lake_root, _verdict(name))
    _flip(lake_root, b'"third"', b'"thi\xffd"')

    with pytest.raises(LedgerNotUtf8) as refusal:
        read_quarantine(lake_root)

    message = str(refusal.value)
    assert "on line 3" in message, message
    assert "0xff" in message, message
    assert "human's job under the lock" in message, message


def test_a_damaged_byte_in_the_last_line_refuses_rather_than_reading_as_a_torn_tail(lake_root):
    """A torn tail is discarded on purpose, and this is not one.

    ``parse_jsonl`` drops a trailing line it cannot parse because a torn write did not finish.
    No torn write can produce these bytes: every prefix of a line ``append_line`` emits is
    valid UTF-8. So a last line that does not decode is damage rather than an unfinished
    write, and reading it as a tail would drop a whole verdict silently.
    """
    append_quarantine(lake_root, _verdict("kept"))
    # Written without a terminating newline, which is what a crash mid-append leaves. A
    # newline-terminated last line is not a torn tail at all, so a fixture built that way
    # cannot tell refusing apart from discarding, and the decision this test exists to hold
    # would be held by nothing. The review lens found exactly that.
    with quarantine_path(lake_root).open("ab") as handle:
        handle.write(b'{"partition": "la\xfft", "verdict": "quarantined", "check": "e"}')

    assert not quarantine_path(lake_root).read_bytes().endswith(b"\n"), (
        "the fixture stopped being a torn tail"
    )
    with pytest.raises(LedgerNotUtf8):
        read_quarantine(lake_root)


def test_a_hand_repaired_ledger_may_hold_non_ascii_and_still_reads(lake_root):
    """The accepting side of the boundary, which is where narrowing it would do the damage.

    No writer here emits a byte outside ASCII, and the test below holds that. The tempting next
    step is to decode as ASCII, since nothing the tree writes would notice. It would be wrong.
    Repairing this file is a hand edit under the lock, which every message about a damaged
    ledger says, and a person writing a reason by hand writes the characters their language
    has. Decoding as ASCII would refuse the ledger a human had just fixed.

    The review lens found this by mutation: ``decode("utf-8")`` narrowed to ``decode("ascii")``
    passed all 4,241 tests in the suite.
    """
    reason = "vendor said \u201chalt\u201d for C\u00e9line"
    entry = {"partition": "p", "check": "e", "verdict": "quarantined", "reason": reason}
    raw = json.dumps(entry, sort_keys=True, ensure_ascii=False) + "\n"
    quarantine_path(lake_root).write_bytes(raw.encode("utf-8"))

    assert max(quarantine_path(lake_root).read_bytes()) > 127, "the fixture stopped being the case"
    assert read_quarantine(lake_root)[0]["reason"] == reason


def test_the_refusal_names_the_lead_byte_of_a_truncated_sequence(lake_root):
    """A multi-byte sequence cut short, where the offending byte is not the last one.

    Every other case here damages a byte with ``0xff``, an invalid start byte, and for those
    ``UnicodeDecodeError`` reports ``start`` and ``end`` one apart, so the lead byte and the
    last byte of the bad run are the same byte. A truncated three-byte sequence separates
    them: ``b"\xe0\xa0"`` reports ``start=0`` and ``end=2``, so a message reading ``end - 1``
    would name ``0xa0`` while the byte that actually refused is ``0xe0``. The number's job is
    to send a person to a byte, and naming the wrong one sends them to the wrong byte.

    The review lens found this by mutation: with only the ``0xff`` cases here, indexing at
    ``end - 1`` passed all 115 tests in the two files this change touches.
    """
    append_quarantine(lake_root, _verdict("kept"))
    _flip(lake_root, b'"quarantined"', b'"quarantin\xe0\xa0"')

    with pytest.raises(LedgerNotUtf8) as refusal:
        read_quarantine(lake_root)

    message = str(refusal.value)
    assert "0xe0" in message, message
    assert "0xa0" not in message, message


def test_every_quarantine_reader_funnels_through_the_decode_refusal_too(lake_root):
    """The same funnel ``TornLedger`` has, for the same reason.

    ``latest_quarantine`` and ``latest_quarantine_by_check`` are what ``loader``, ``sweep``,
    ``dashboard`` and ``signoff`` actually call. A refusal only ``read_quarantine`` made would
    leave all four meeting the bare ``UnicodeDecodeError``.
    """
    append_quarantine(lake_root, _verdict("kept"))
    _flip(lake_root, b'"quarantined"', b'"quarantin\xffd"')

    for reader in (read_quarantine, latest_quarantine, latest_quarantine_by_check):
        with pytest.raises(LedgerNotUtf8):
            reader(lake_root)


def test_the_refusal_is_not_a_replacement_because_replacing_inverts_the_guard(lake_root):
    """Why this refuses instead of decoding with ``errors="replace"``, which is one line.

    A replacement character inside a JSON string leaves the line **valid JSON** with one field
    silently rewritten. When that field is ``partition``, the entry files under a key no reader
    asks about, so the partition the verdict withholds disappears from the ledger and reads
    clean. The assertions below are what a replacement would produce, stated as the thing that
    must not happen.
    """
    held = "chains/ticker=SPY/date=2026-09-14.parquet"
    append_quarantine(lake_root, _verdict(held))
    _flip(lake_root, b"2026-09-14", b"2026-09-\xff4")

    with pytest.raises(LedgerNotUtf8):
        latest_quarantine(lake_root)

    # The two facts a replacement would establish instead, both of them wrong.
    replaced = quarantine_path(lake_root).read_bytes().decode("utf-8", "replace")
    entries = json.loads(replaced.splitlines()[0])
    assert entries["partition"] != held, "the premise of this test no longer holds"
    assert json.loads(replaced.splitlines()[0])["verdict"] == "quarantined"


def test_append_line_emits_pure_ascii_and_every_prefix_of_it_decodes(lake_root):
    """What the refusal's own message tells the operator, held as a test.

    The message says these bytes were changed by something other than a writer, and the
    reachability argument on marketlake #495 rests on the same fact. ``json.dumps`` runs with
    ``ensure_ascii`` at its default, and flipping that default is a one-word edit in
    ``append_line`` that nothing else would notice.

    This holds the one writer rather than the claim that it is the only one. Every writer above
    it funnels here, through ``manifest.append_quarantine``, ``battery.write_verdict`` and
    ``lake.signoff``, and that funnel is a fact about call sites that no test can keep true.

    The second half is what rules out a torn write: ``os.write`` can stop between bytes but
    never inside one, so if every prefix of a written line decodes then no crash mid-append can
    produce this shape.
    """
    reason = "vendor said \u201cdelayed\u201d \u00e9 \U0001f600"
    append_line(quarantine_path(lake_root), {"partition": "p", "check": "e", "reason": reason})
    raw = quarantine_path(lake_root).read_bytes()

    assert max(raw) < 128, f"a writer emitted a byte outside ASCII: {raw!r}"
    for cut in range(len(raw) + 1):
        raw[:cut].decode("utf-8")


def test_a_lake_with_no_ledger_and_an_empty_one_both_read_as_no_entries(lake_root):
    """Tonight's first run. The battery has never written, so nothing may refuse."""
    assert read_quarantine(lake_root) == []
    assert latest_quarantine(lake_root) == {}

    quarantine_path(lake_root).write_text("")
    assert read_quarantine(lake_root) == []
    assert latest_quarantine(lake_root) == {}


def test_the_manifest_ledger_keeps_its_truncating_read(lake_root):
    """The boundary against marketlake #447, pinned so it cannot widen unnoticed.

    ``scrub`` resolves the manifest through ``latest_entries``, so raising here would take
    the Sunday scrub down on the very file it exists to report. The quarantine ledger's
    readers are a guard and refuse. The manifest's read short, and #447 owns that half.
    """
    append_manifest(
        lake_root, partition="a", source="capture", sha256="s1", rows=1, fetched_at=None
    )
    raw = manifest_path(lake_root).read_text()
    manifest_path(lake_root).write_text(raw.rstrip("\n")[:-20])
    append_line(manifest_path(lake_root), {"partition": "b", "sha256": "s2", "rows": 2})
    append_line(manifest_path(lake_root), {"partition": "c", "sha256": "s3", "rows": 3})

    assert read_manifest(lake_root) == []
    assert latest_entries(lake_root) == {}
    assert scrub(lake_root).ok


def test_an_append_in_flight_never_refuses_a_ledger_nothing_is_wrong_with(lake_root):
    """What makes failing closed safe, given that ``loader._guard`` takes no lock.

    ``append_line`` writes one line in one ``O_APPEND`` write, so a reader landing mid-write
    can only ever see a partial *last* line, which is the tail this does not count. A line
    in the body needs a later append to have already landed behind a fragment, which is
    damage that is permanent rather than transient. Without this property a fail-closed
    guard on an unsynchronised read would refuse healthy lakes at random.
    """
    path = quarantine_path(lake_root)
    path.write_text("")
    appends = 1500
    done = threading.Event()
    refusals: list[Exception] = []
    reads = []

    def writer() -> None:
        try:
            for i in range(appends):
                append_line(path, dict(_verdict(f"p{i}"), reason="x" * 300))
        finally:
            done.set()

    def reader() -> None:
        while not done.is_set():
            try:
                reads.append(len(read_quarantine(lake_root)))
            except TornLedger as exc:
                refusals.append(exc)

    threads = [threading.Thread(target=writer)] + [
        threading.Thread(target=reader) for _ in range(3)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not refusals, f"{len(refusals)} reads refused a ledger nothing is wrong with"
    assert reads, "no read completed, so this test proves nothing"
    assert len(read_quarantine(lake_root)) == appends
