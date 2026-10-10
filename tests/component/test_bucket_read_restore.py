"""Restoring a chosen range from the bucket into an empty directory, for reading.

Marketlake #837. ``python -m lake.bucket restore-for-reading`` restores a range of chains or
quotes partitions, with what a reader needs beside it, into a directory outside the live lake,
so a session the VM's trim removed can be read there. The cases follow the issue's
Verification list.

1. A range restore of a trimmed bucket holds exactly the planned set, and each view reads the
   same answer from the reading directory as from the untrimmed source lake, bounded at the
   range.
2. A bad key outside the plan fails nothing, and the same key inside it fails the run.
3. A destination that is not empty, or sits inside ``lake_root``, refuses and writes nothing.
4. The journal reserve applies only when the destination shares ``lake_root``'s filesystem,
   and on any other filesystem the read must leave a 1 GB floor free instead.
5. The verified marker records the mode and the range, so a waiting directory moves in only
   for the run that made it. Refusals on the arguments and on an empty range come first.
6. The command runs on a shadow host.
7. The command hands its config and arguments through, and prints one line per fact.

The inventory bucket is built by the real ``first_upload``, then the range restore tests'
``_trim_away``, then the real ``nightly_upload``, so it holds what an uploaded trimmed lake
holds. Each view's test builds its lake with that view's own test module's helpers, uploads it
with the real ``first_upload`` and compares the two answers. Expected bytes and sizes are read
from the fake bucket's store or written out here, never through the code under test. Each
test's docstring names the mutation it catches.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from lake import bucket, runway
from lake.bucket import RestoreRefused, first_upload, nightly_upload, restore_for_reading
from lake.calendar import MARKET_TZ
from lake.capture_spans import CaptureSpans
from lake.config import BucketTarget
from lake.continuity import continuity_view
from lake.loader import PartitionQuarantined, load_chain, load_contract_life
from lake.manifest import append_manifest
from lake.oi import (
    REASON_NOT_YET_CAPTURED,
    REASON_PARTITION_ABSENT,
    VERDICT_ABSENT,
    VERDICT_PENDING,
    VERDICT_SETTLED,
)
from lake.settle import settlement_view
from tests.component import test_continuity_view as continuity
from tests.component import test_load_chain as chain
from tests.component import test_load_contract_life as life
from tests.component import test_oi_view as oi
from tests.component import test_settlement_view as settle
from tests.component.test_bucket_range_restore import _trim_away
from tests.support.bucket import FakeS3, unreachable
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.lake import FixtureLake, sample_chains_table

TARGET = BucketTarget(bucket="lake-backup", prefix="lake")
D1 = date(2026, 8, 24)
D2 = date(2026, 8, 25)
D3 = date(2026, 8, 26)
D4 = date(2026, 8, 27)
D5 = date(2026, 8, 28)
WORK = ".marketlake-restoring"
FRIDAY_19 = datetime(2026, 8, 28, 19, 0, tzinfo=MARKET_TZ)
MONDAY_19 = datetime(2026, 8, 31, 19, 0, tzinfo=MARKET_TZ)
CALENDAR = weekday_sessions(date(2026, 8, 24), date(2026, 8, 31))
PLENTY = 10**12
RESERVE_SESSIONS = 13
FLOOR = 1_000_000_000
LABEL = "restore-for-reading"


def _chains(ticker: str, day: date) -> str:
    return f"chains/ticker={ticker}/date={day.isoformat()}.parquet"


def _bars(ticker: str, freq: str, day: date) -> str:
    return f"bars/ticker={ticker}/freq={freq}/date={day.isoformat()}.parquet"


QUOTES_D2 = "quotes/ticker=SPY/date=2026-08-25.parquet"
ACTIONS = "actions/corporate_actions.jsonl"
QUARANTINE = "quarantine.jsonl"
MASTER = "reference/security_master.parquet"
SPANS = "reference/capture_spans.parquet"
LEDGER = "reference/schema_versions.parquet"
# A file under reference/ the manifest does not record, which a reading restore never plans.
UNRECORDED = "reference/notes.parquet"
REPORT = "reports/close_guard/date=2026-08-25/run.json"
SEGMENT = "journal/date=2026-08-26/surface=chains/ticker=QQQ/seg-20260826T133000Z-4242.arrows"


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _files(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _gets(client: FakeS3) -> list[str]:
    return [kwargs["Key"] for name, kwargs in client.calls if name == "get_object"]


def _bucket_files(client: FakeS3, rels) -> dict[str, bytes]:
    """The bucket's current bytes for each of ``rels``, read off the fake's own store."""
    return {rel: client.body(f"lake/{rel}") for rel in rels}


def _record_in_bucket(client: FakeS3, rel: str, data: bytes) -> None:
    """Store ``data`` at ``rel`` and append its entry to the bucket's manifest, as an upload."""
    client.store(f"lake/{rel}", data)
    entry = {"fetched_at": None, "partition": rel, "rows": 1, "sha256": _sha(data), "source": "x"}
    raw = client.body("lake/manifest.jsonl") + (json.dumps(entry, sort_keys=True) + "\n").encode()
    client.store("lake/manifest.jsonl", raw)


def _upload(root: Path) -> FakeS3:
    client = FakeS3()
    first_upload(root, TARGET, client=client, clock=ManualClock(FRIDAY_19), calendar=CALENDAR)
    return client


def _read(
    client: FakeS3,
    dest: Path,
    lake_root: Path,
    *,
    surface: str = "chains",
    ticker: str | None = "SPY",
    first: date | str = D2,
    last: date | str = D3,
    free: int = PLENTY,
    device_of=None,
) -> bucket.ReadingRestoreSummary:
    kwargs = {} if device_of is None else {"device_of": device_of}
    return restore_for_reading(
        dest,
        TARGET,
        client=client,
        lake_root=lake_root,
        surface=surface,
        ticker=ticker,
        first=first if isinstance(first, date) else date.fromisoformat(first),
        last=last if isinstance(last, date) else date.fromisoformat(last),
        free_space=lambda path: free,
        **kwargs,
    )


# -- 1. the inventory ------------------------------------------------------------------


class Inventory:
    """A lake uploaded, trimmed inside the range, and uploaded again."""

    def __init__(self, root: Path, client: FakeS3, trimmed: bytes) -> None:
        self.root = root
        self.client = client
        # The trimmed partition's bytes, which the lake no longer holds and the bucket does.
        self.trimmed = trimmed


def _inventory(tmp_path: Path) -> Inventory:
    """Every kind of file a reading restore must take or leave, in one bucket.

    Chains for SPY on five days, with the range D2 to D3, the next partition D4 and a later
    D5. A second ticker with a partition inside the range's dates and its own next partition
    on D4, and a quotes partition inside the range's dates. ``1m`` and ``1d`` bars
    inside and outside the range, and a second ticker's. The actions ledger, quarantine, the
    three reference tables, a report, a manifested journal segment and an unmanifested file
    under ``reference/``. Then SPY's D2 is trimmed and the nightly upload carries the trim.
    """
    candles = sample_chains_table()
    lake = FixtureLake(tmp_path / "lake")
    for day in (D1, D2, D3, D4, D5):
        lake.with_chains("SPY", day)
    lake.with_chains("QQQ", D2).with_chains("QQQ", D4).with_quotes("SPY", D2)
    for ticker, freq, day in (
        ("SPY", "1m", D2),
        ("SPY", "1d", D2),
        ("SPY", "1d", D3),
        ("SPY", "1d", D1),
        ("SPY", "1d", D4),
        ("QQQ", "1d", D2),
    ):
        lake.with_bars(ticker, freq, day, candles)
    oi.reference(lake)
    lake.with_quarantine({"partition": _chains("SPY", D3), "verdict": "delayed_feed"})
    lake.with_journal_segment(
        "chains", "QQQ", D3.isoformat(), candles, start_ts="20260826T133000Z", pid=4242
    )
    root = lake.build()
    continuity._append_split(root)
    append_manifest(
        root,
        partition=SEGMENT,
        source="capture",
        sha256=_sha((root / SEGMENT).read_bytes()),
        rows=1,
        fetched_at=None,
    )
    (root / REPORT).parent.mkdir(parents=True)
    (root / REPORT).write_text('{"report": true}\n')
    (root / UNRECORDED).write_bytes(b"not in the manifest")
    client = _upload(root)
    trimmed = (root / _chains("SPY", D2)).read_bytes()
    _trim_away(root, client, _chains("SPY", D2))
    nightly_upload(root, TARGET, client=client, clock=ManualClock(MONDAY_19), calendar=CALENDAR)
    for rel in (UNRECORDED, REPORT, SEGMENT, "trimmed.jsonl", ACTIONS):
        assert f"lake/{rel}" in client.keys(), rel
    assert not (root / _chains("SPY", D2)).exists()
    return Inventory(root, client, trimmed)


PLANNED = (
    _chains("SPY", D2),
    _chains("SPY", D3),
    _chains("SPY", D4),
    _bars("SPY", "1m", D2),
    _bars("SPY", "1d", D2),
    _bars("SPY", "1d", D3),
    ACTIONS,
    QUARANTINE,
    MASTER,
    SPANS,
    LEDGER,
    "manifest.jsonl",
)


def test_a_reading_restore_holds_the_range_and_what_a_reader_needs_and_nothing_else(tmp_path):
    """The exact file set, the trimmed partition's own bytes, and only planned downloads.

    Catches leaving out ``reference/``, ``quarantine.jsonl``, the bars, the actions ledger or
    the next partition, and planning from the listing, which would take the report, the
    segment, the unrecorded file, the trimmed ledger and every other ticker and day. Catches
    too a next partition taken for a ticker the range did not select, which would bring QQQ's
    D4.
    """
    lake = _inventory(tmp_path)
    dest = tmp_path / "reading"
    lake.client.calls.clear()

    summary = _read(lake.client, dest, lake.root)

    assert summary.restored is True
    assert summary.failures == []
    assert _files(dest) == _bucket_files(lake.client, PLANNED)
    assert (dest / _chains("SPY", D2)).read_bytes() == lake.trimmed
    assert sorted(_gets(lake.client)) == sorted(f"lake/{rel}" for rel in PLANNED)
    assert summary.range_by_ticker == {"SPY": 2}
    assert summary.render() == (
        "restored for reading 2 partition(s) in the range (SPY 2), 1 next chains partition(s) "
        "after it, 3 bars partition(s), and 6 support file(s) (actions/corporate_actions.jsonl, "
        "manifest.jsonl, quarantine.jsonl, reference/capture_spans.parquet, "
        "reference/schema_versions.parquet, reference/security_master.parquet) from "
        f"s3://lake-backup/lake into {dest}: downloaded 11 (0.0 MB), 0 already verified in the "
        f"working directory. Read it with lake_root={dest}, and never make it a daemon's or a "
        "job's lake_root, since its manifest records partitions it does not hold"
    )


def test_every_ticker_takes_each_ticker_s_range_and_bars(tmp_path):
    """Leaving out ``--ticker`` takes QQQ's partition, bars and next partition too.

    Catches a selection that ignores ``ticker=None`` and a bars plan built from the argument
    rather than from the tickers the range selected.
    """
    lake = _inventory(tmp_path)
    dest = tmp_path / "reading"

    summary = _read(lake.client, dest, lake.root, ticker=None)

    extra = (_chains("QQQ", D2), _chains("QQQ", D4), _bars("QQQ", "1d", D2))
    assert _files(dest) == _bucket_files(lake.client, (*PLANNED, *extra))
    assert summary.range_by_ticker == {"QQQ": 1, "SPY": 2}
    assert summary.count("next") == 2


def test_a_quotes_range_takes_no_next_chains_partition(tmp_path):
    """The next partition is for ``oi_view``, which reads chains only.

    Catches the next-partition rule applied to every surface.
    """
    lake = _inventory(tmp_path)
    dest = tmp_path / "reading"

    summary = _read(lake.client, dest, lake.root, surface="quotes")

    support = (ACTIONS, QUARANTINE, MASTER, SPANS, LEDGER, "manifest.jsonl")
    bars = (_bars("SPY", "1m", D2), _bars("SPY", "1d", D2), _bars("SPY", "1d", D3))
    assert _files(dest) == _bucket_files(lake.client, (QUOTES_D2, *bars, *support))
    assert summary.count("next") == 0


def test_the_next_partition_is_a_chains_one_past_a_quotes_partition_in_a_chains_gap(tmp_path):
    """SPY has chains on D2 and D4 and quotes on D3, so D4's chains is the next partition.

    Catches the next-partition search taking any surface, which would take D3's quotes, the
    earliest partition past the range, and leave D4's chains out.
    """
    lake = FixtureLake(tmp_path / "lake")
    for day in (D1, D2, D4):
        lake.with_chains("SPY", day, sample_chains_table())
    lake.with_quotes("SPY", D3)
    lake.with_reference("schema_versions", chain._ledger_table())
    root = lake.build()
    client = _upload(root)
    dest = tmp_path / "reading"

    summary = _read(client, dest, root, first=D2, last=D2)

    assert _files(dest) == _bucket_files(
        client, (_chains("SPY", D2), _chains("SPY", D4), LEDGER, "manifest.jsonl")
    )
    assert summary.count("next") == 1


# -- 1. each view reads the same answer --------------------------------------------------


def test_load_chain_reads_a_trimmed_range_as_the_untrimmed_lake_did(tmp_path):
    """``load_chain`` on both sessions of the range, after the trim removed both from the lake.

    The answers are read from the source lake before the trim. Then both partitions are trimmed
    with the range restore tests' ``_trim_away``, and the real ``nightly_upload`` carries the
    trim, so the bucket holds what an uploaded trimmed lake holds and the lake holds neither
    session. One of the two is quarantined, so it is withheld and comes back only with
    ``include_quarantined=True``.

    Catches leaving out ``quarantine.jsonl``, where the reading directory serves the flagged
    partition with no error, leaving out ``reference/``, where the schema-version ledger is
    missing and the read raises ``PartialRead``, and a plan that takes only what the live lake
    still holds, where both sessions read as absent.
    """
    source = chain._lake(
        FixtureLake(tmp_path / "lake"),
        quarantine=[{"partition": chain.FULL_PARTITION, "verdict": "delayed_feed"}],
    )
    with pytest.raises(PartitionQuarantined):
        load_chain("SPY", chain.FULL_DAY, lake_root=source)
    half = load_chain("SPY", chain.HALF_DAY, lake_root=source)
    full = load_chain("SPY", chain.FULL_DAY, lake_root=source, include_quarantined=True)
    client = _upload(source)
    trimmed = (chain.FULL_PARTITION, f"chains/ticker=SPY/date={chain.HALF_DAY}.parquet")
    for rel in trimmed:
        _trim_away(source, client, rel)
    nightly_upload(source, TARGET, client=client, clock=ManualClock(MONDAY_19), calendar=CALENDAR)
    assert not any((source / rel).exists() for rel in trimmed)
    assert "lake/trimmed.jsonl" in client.keys()
    dest = tmp_path / "reading"

    _read(client, dest, source, first=chain.FULL_DAY, last=chain.HALF_DAY)

    with pytest.raises(PartitionQuarantined):
        load_chain("SPY", chain.FULL_DAY, lake_root=dest)
    assert load_chain("SPY", chain.HALF_DAY, lake_root=dest).equals(half)
    assert load_chain("SPY", chain.FULL_DAY, lake_root=dest, include_quarantined=True).equals(full)


def test_settlement_view_reads_the_session_as_the_source_lake_does(tmp_path):
    """``settlement_view`` reads the chains and the session's ``1d`` bars.

    Catches leaving out the bars, where the reading directory raises ``BarsAbsent``.
    """
    source = settle._lake(
        FixtureLake(tmp_path / "lake"),
        [settle._contract(750.0), settle._contract(760.0, put_call="PUT")],
    )
    client = _upload(source)
    dest = tmp_path / "reading"

    _read(client, dest, source, first=settle.SESSION, last=settle.SESSION)

    answer = settlement_view(settle.TICKER, settle.SESSION, lake_root=dest)
    assert answer.num_rows == 2
    assert answer.equals(settlement_view(settle.TICKER, settle.SESSION, lake_root=source))


def _continuity_source(tmp_path: Path) -> Path:
    """The continuity view's split lake, with a fourth session after the range.

    The master and the capture spans are manifested through ``with_reference`` rather than
    written after ``build`` as that module's ``_lake`` writes its master, since an unmanifested
    master never reaches a reading directory. The split is appended after ``build``, which
    rewrites the manifest whole.
    """
    s4 = "2026-09-17"
    master = continuity._master()
    spans = CaptureSpans()
    spans.open_span(continuity.EQUITY, datetime(2026, 9, 8, tzinfo=UTC), True)
    lake = FixtureLake(tmp_path / "lake")
    lake.with_reference("security_master", master.to_table())
    lake.with_reference("capture_spans", spans.to_table())
    chains = continuity._adjusted_chains()
    chains[s4] = [
        continuity._contract(
            s4, continuity.NEW, strike=continuity.STRIKE_AFTER, units=continuity.UNITS_AFTER
        )
    ]
    bars = {day: [continuity._bar(day, close)] for day, close in continuity.CLOSES.items()}
    bars[s4] = [continuity._bar(s4, 223.0)]
    return continuity._lake(lake, chains, bars=bars, split=True)


def test_continuity_view_reads_across_the_split_as_the_source_lake_does(tmp_path):
    """``continuity_view`` across a split inside the range, with ``end`` at its last day.

    Catches leaving out the actions ledger, where every session before the boundary loses its
    split ratio, and leaving out the bars, where every row is marked for a missing close.
    """
    source = _continuity_source(tmp_path)
    client = _upload(source)
    dest = tmp_path / "reading"

    summary = _read(client, dest, source, first=continuity.S1, last=continuity.S3)

    assert summary.count("next") == 1
    answer = continuity_view(continuity.TICKER, continuity.OLD, end=continuity.S3, lake_root=dest)
    assert [row["split_ratio"] for row in answer.to_pylist()] == [continuity.RATIO] * 2 + [1.0]
    assert {row["verdict"] for row in answer.to_pylist()} == {continuity.VERDICT_SETTLED}
    assert answer.equals(
        continuity_view(continuity.TICKER, continuity.OLD, end=continuity.S3, lake_root=source)
    )


def test_load_contract_life_reads_the_range_as_the_source_lake_does(tmp_path):
    """``load_contract_life`` with ``end`` at the range's last day, across the remap.

    The range ends on ``SEALED[3]``, the boundary where the contract was re-symboled, so one of
    its four rows is reached only through the master.

    Catches leaving out ``reference/``, where the master that threads the contract is missing
    and the read finds the old symbol's three rows alone.
    """
    source = life._lake(FixtureLake(tmp_path / "lake"), master=life._master())
    client = _upload(source)
    dest = tmp_path / "reading"
    last = life.SEALED[3]

    _read(client, dest, source, first=life.SEALED[0], last=last)

    answer = load_contract_life(life.OLD, end=last, lake_root=dest)
    assert answer.num_rows == 4
    assert answer.equals(load_contract_life(life.OLD, end=last, lake_root=source))


def test_oi_view_on_the_range_s_last_day_is_settled_when_the_next_session_is_sealed(tmp_path):
    """The next partition is the calendar-next session, so the last day reads settled.

    Catches leaving out the next partition, where the reading directory answers ``pending``.
    """
    source = oi.settled_lake(
        FixtureLake(tmp_path / "lake"),
        [
            oi.cycle_rows(oi.FOLLOWING, 9, 30, oi.REFRESHED, volumes=oi.VOLUMES),
            oi.cycle_rows(oi.FOLLOWING, 9, 31, oi.REFRESHED, volumes=oi.VOLUMES),
        ],
    )
    client = _upload(source)
    dest = tmp_path / "reading"

    _read(client, dest, source, first=oi.SESSION, last=oi.SESSION)

    answer = oi.view(dest, constants=oi.constants())
    assert oi.verdicts(answer) == {(VERDICT_SETTLED, None)}
    assert answer.equals(oi.view(source, constants=oi.constants()))


def test_oi_view_after_a_capture_gap_is_absent_as_on_the_source_lake(tmp_path):
    """The next partition follows a gap, so the last day reads absent, as on the full lake.

    Catches leaving out the next partition, where the reading directory answers ``pending``,
    and taking the calendar-next session instead of the next the manifest records, which the
    bucket does not hold.
    """
    lake = FixtureLake(tmp_path / "lake")
    lake.with_chains(
        "SPY", oi.SESSION, oi.table(oi.close_rows(oi.SESSION, oi.SET, volumes=oi.VOLUMES))
    )
    lake.with_chains(
        "SPY", oi.THIRD, oi.table(oi.cycle_rows(oi.THIRD, 9, 30, oi.REFRESHED, volumes=oi.VOLUMES))
    )
    oi.reference(lake)
    source = lake.build()
    client = _upload(source)
    dest = tmp_path / "reading"

    _read(client, dest, source, first=oi.SESSION, last=oi.SESSION)

    answer = oi.view(dest, constants=oi.constants())
    assert oi.verdicts(answer) == {(VERDICT_ABSENT, REASON_PARTITION_ABSENT)}
    assert answer.equals(oi.view(source, constants=oi.constants()))


def test_without_the_next_partition_the_last_day_would_read_pending(tmp_path):
    """The probe the next-partition rule rests on: the range alone answers ``pending``.

    It deletes the next partition from a finished reading directory, so it shows what the
    rule prevents rather than testing the rule itself.
    """
    source = oi.settled_lake(
        FixtureLake(tmp_path / "lake"),
        [oi.cycle_rows(oi.FOLLOWING, 9, 30, oi.REFRESHED, volumes=oi.VOLUMES)],
    )
    client = _upload(source)
    dest = tmp_path / "reading"
    _read(client, dest, source, first=oi.SESSION, last=oi.SESSION)

    (dest / f"chains/ticker=SPY/date={oi.FOLLOWING}.parquet").unlink()

    answer = oi.view(dest, constants=oi.constants())
    assert oi.verdicts(answer) == {(VERDICT_PENDING, REASON_NOT_YET_CAPTURED)}


# -- 2. which keys fail the run ----------------------------------------------------------


def _simple(tmp_path: Path) -> tuple[Path, FakeS3]:
    """SPY chains on D1 to D4 and QQQ on D2, with the schema ledger, uploaded."""
    lake = FixtureLake(tmp_path / "lake")
    for day in (D1, D2, D3, D4):
        lake.with_chains("SPY", day, sample_chains_table())
    lake.with_chains("QQQ", D2)
    lake.with_reference("schema_versions", chain._ledger_table())
    root = lake.build()
    return root, _upload(root)


def test_bad_keys_outside_the_plan_fail_nothing(tmp_path):
    """An unsafe key, a case collision and a lost manifest entry, all outside the range.

    Catches running the three bucket-wide checks of the ``restore`` command over the whole
    listing rather than over the plan.
    """
    root, client = _simple(tmp_path)
    client.store("lake/reports/../escape.json", b"x")
    client.store("lake/reports/A.json", b"a")
    client.store("lake/reports/a.json", b"a")
    del client.objects[f"lake/{_chains('SPY', D1)}"]
    # The whole-lake restore fails on this bucket, which is what makes the test bite.
    assert bucket.restore_lake(tmp_path / "whole", TARGET, client=client).restored is False

    summary = _read(client, tmp_path / "reading", root)

    assert summary.restored is True
    assert summary.failures == []


@pytest.mark.parametrize(
    ("damage", "rel", "why"),
    [
        pytest.param(
            lambda client: _record_in_bucket(client, "reference/../escape.parquet", b"x"),
            "reference/../escape.parquet",
            "names a path outside the lake, so it was not written",
            id="unsafe",
        ),
        pytest.param(
            lambda client: (
                _record_in_bucket(client, "reference/Notes.parquet", b"a"),
                _record_in_bucket(client, "reference/notes.parquet", b"b"),
            ),
            "reference/notes.parquet",
            "differs from reference/Notes.parquet only by case, so one would overwrite the "
            "other on a filesystem that ignores case",
            id="case",
        ),
        pytest.param(
            lambda client: client.objects.pop(f"lake/{_chains('SPY', D3)}"),
            _chains("SPY", D3),
            "missing from the bucket",
            id="missing",
        ),
    ],
)
def test_a_bad_key_inside_the_plan_fails_the_run_and_moves_nothing(
    tmp_path, monkeypatch, capsys, damage, rel, why
):
    """Each of the three, inside the plan, exits 1 through ``main`` with nothing moved.

    Catches dropping any one of the three checks from the reading restore.
    """
    root, client = _simple(tmp_path)
    damage(client)
    dest = tmp_path / "reading"

    code = _main(tmp_path, root, client, monkeypatch, [str(dest), "--surface", "chains"])

    assert code == 1
    err = capsys.readouterr().err.splitlines()
    assert any(line.startswith(f"{LABEL}: {why}: {rel}, ") for line in err), err
    assert err[-1] == (
        f"{LABEL}: 1 file(s) from s3://lake-backup/lake failed, so {dest} holds no reading set. "
        f"Every file that verified stays in {dest / WORK}, and a re-run resumes there"
    )
    assert sorted(os.listdir(dest)) == [WORK]


def test_a_mismatched_file_names_its_role_and_both_causes(tmp_path, monkeypatch, capsys):
    """A range partition whose current version does not match its entry.

    Catches a failure line that drops what the file is to the range, or the causes.
    """
    root, client = _simple(tmp_path)
    client.store(f"lake/{_chains('SPY', D3)}", b"rot")
    dest = tmp_path / "reading"

    code = _main(tmp_path, root, client, monkeypatch, [str(dest), "--surface", "chains"])

    assert code == 1
    err = capsys.readouterr().err.splitlines()
    assert err[0] == (
        f"{LABEL}: does not match its SHA-256: {_chains('SPY', D3)}, a partition in the range"
    )
    assert "a run after the next complete nightly upload passes" in err[1]
    assert '"When the lake is gone"' in err[1] and str(dest / WORK) in err[1]
    assert "When the bucket holds an earlier good version" in err[1]
    assert len(err) == 3


def test_each_role_has_its_own_failure_line_and_a_missing_file_prints_no_causes(
    tmp_path, monkeypatch, capsys
):
    """A bars partition, the next partition and a support file, each missing from the bucket.

    Catches a role's words dropped from the failure line, and the line naming the two causes
    of a mismatch printed on a run where no file mismatched.
    """
    lake = _inventory(tmp_path)
    dest = tmp_path / "reading"
    for rel in (_bars("SPY", "1d", D3), _chains("SPY", D4), MASTER):
        lake.client.objects.pop(f"lake/{rel}")

    code = _main(tmp_path, lake.root, lake.client, monkeypatch, [str(dest), "--surface", "chains"])

    assert code == 1
    assert capsys.readouterr().err.splitlines() == [
        f"{LABEL}: missing from the bucket: {_bars('SPY', '1d', D3)}, a bars partition of the "
        "range's tickers and days",
        f"{LABEL}: missing from the bucket: {_chains('SPY', D4)}, the next chains partition "
        "after the range",
        f"{LABEL}: missing from the bucket: {MASTER}, a file a reader needs beside the range",
        f"{LABEL}: 3 file(s) from s3://lake-backup/lake failed, so {dest} holds no reading set. "
        f"Every file that verified stays in {dest / WORK}, and a re-run resumes there",
    ]


def test_a_symbolic_link_inside_the_working_directory_is_never_written_through(tmp_path):
    """A ``chains`` link in a working directory a restore made points outside it.

    Catches the plan checking only the key's spelling and not where it resolves, which
    downloads every chains partition into the directory the link names.
    """
    root, client = _simple(tmp_path)
    dest = tmp_path / "reading"
    work = dest / WORK
    work.mkdir(parents=True)
    (work / ".marketlake-restore").write_text("a marketlake restore in progress\n")
    outside = tmp_path / "outside"
    outside.mkdir()
    (work / "chains").symlink_to(outside)

    summary = _read(client, dest, root)

    why = "names a path outside the lake, so it was not written"
    assert summary.restored is False
    assert summary.failures == [
        (_chains("SPY", D2), why),
        (_chains("SPY", D3), why),
        (_chains("SPY", D4), why),
    ]
    assert os.listdir(outside) == []


def test_a_key_that_vanishes_during_its_download_leaves_no_in_flight_file(tmp_path):
    """The listing holds D3's key and the download finds it gone.

    Catches the absent branch keeping the empty ``.part`` file the download opened.
    """
    root, client = _simple(tmp_path)
    key = f"lake/{_chains('SPY', D3)}"

    def vanish(kwargs) -> None:
        if kwargs["Key"] == key:
            client.objects.pop(key, None)

    client.on_get = vanish
    dest = tmp_path / "reading"

    summary = _read(client, dest, root)

    assert summary.failures == [(_chains("SPY", D3), "missing from the bucket")]
    assert list((dest / WORK).rglob("*.part")) == []
    assert not (dest / WORK / _chains("SPY", D3)).exists()


# -- 3. destinations that refuse ---------------------------------------------------------


def test_a_destination_that_is_not_empty_refuses_and_writes_nothing(tmp_path):
    """Catches a reading restore that skips ``_destination_state``."""
    root, client = _simple(tmp_path)
    dest = tmp_path / "reading"
    dest.mkdir()
    (dest / "keep.txt").write_text("mine")
    client.calls.clear()

    with pytest.raises(RestoreRefused, match="is not empty, it holds keep.txt"):
        _read(client, dest, root)

    assert sorted(os.listdir(dest)) == ["keep.txt"]
    assert client.calls == []


@pytest.mark.parametrize("inside", ["root", "under"])
def test_a_destination_inside_lake_root_refuses_and_writes_nothing(tmp_path, inside):
    """``lake_root`` itself, and an absent directory under it, refuse before any request.

    Catches dropping the check, where an absent directory under the live lake is created and
    filled, and the root itself refuses only as not empty.
    """
    root, client = _simple(tmp_path)
    dest = root if inside == "root" else root / "reading"
    before = _files(root)
    client.calls.clear()

    with pytest.raises(RestoreRefused) as refused:
        _read(client, dest, root)

    assert str(refused.value) == (
        f"{dest} is inside lake_root {root}. A reading restore writes only outside the live "
        "lake, so nothing was restored. Name a directory elsewhere"
    )
    assert _files(root) == before
    assert not (root / "reading").exists()
    assert client.calls == []


def test_a_destination_under_lake_root_spelled_in_another_case_refuses(tmp_path):
    """On a filesystem that ignores case, ``LAKE`` and ``lake`` are one directory.

    Catches comparing resolved paths, which ``realpath`` leaves in the spelling given.
    """
    root, client = _simple(tmp_path)
    shouted = root.with_name(root.name.upper())
    if not shouted.exists():
        pytest.skip("this filesystem tells LAKE from lake, so the two are different directories")
    dest = root / "reading"

    with pytest.raises(RestoreRefused, match="is inside lake_root"):
        _read(client, dest, shouted)

    assert not dest.exists()


def test_a_lake_root_that_cannot_be_statted_falls_back_to_the_resolved_path(tmp_path):
    """A missing ``lake_root`` still refuses a destination under its path.

    Catches a check that passes whenever ``lake_root`` cannot be statted, which then reaches
    ``_destination_state`` and refuses for a missing parent instead.
    """
    _root, client = _simple(tmp_path)
    gone = tmp_path / "gone"

    with pytest.raises(RestoreRefused, match="is inside lake_root"):
        _read(client, gone / "reading", gone)


@pytest.mark.parametrize("inside", ["root", "under"])
def test_a_missing_lake_root_behind_a_symbolic_link_still_refuses(tmp_path, inside):
    """A missing ``lake_root`` spelled through a linked parent refuses itself and what is under it.

    The destination resolves through the link, so the fallback must resolve ``lake_root`` too.
    Catches a fallback that compares against ``lake_root`` as spelled, which reads the
    destination as outside and restores into the path the live lake would take.
    """
    _root, client = _simple(tmp_path)
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real)
    lake_root = link / "gone"
    dest = lake_root if inside == "root" else lake_root / "reading"
    client.calls.clear()

    with pytest.raises(RestoreRefused, match="is inside lake_root"):
        _read(client, dest, lake_root)

    assert os.listdir(real) == []
    assert client.calls == []


def test_a_destination_that_is_a_symbolic_link_loop_is_one_line(tmp_path):
    """Catches the inside check letting a loop's resolve error out as a traceback."""
    root, client = _simple(tmp_path)
    loop = tmp_path / "loop"
    loop.symlink_to(loop)

    with pytest.raises(RestoreRefused, match="so nothing was restored"):
        _read(client, loop, root)


def test_a_destination_beside_lake_root_with_a_shared_prefix_is_outside(tmp_path):
    """``lake-reading`` beside ``lake`` is not inside it, which a string prefix would say."""
    root, client = _simple(tmp_path)

    summary = _read(client, tmp_path / "lake-reading", root)

    assert summary.restored is True


def test_a_destination_whose_ancestor_cannot_be_statted_is_one_line(tmp_path):
    """A destination under a directory nobody may search refuses before any request.

    Catches the inside check's ``OSError`` escaping as a traceback.
    """
    _skip_as_root()
    root, client = _simple(tmp_path)
    locked = tmp_path / "locked"
    locked.mkdir()
    dest = locked / "inner" / "reading"
    locked.chmod(0)
    client.calls.clear()
    try:
        with pytest.raises(RestoreRefused) as refused:
            _read(client, dest, root)
    finally:
        locked.chmod(0o755)

    message = str(refused.value)
    assert message.startswith(
        f"checking whether {dest} is inside lake_root {root} failed (PermissionError: "
    )
    assert message.endswith(", so nothing was restored")
    assert "\n" not in message
    assert client.calls == []
    assert os.listdir(locked) == []


def test_a_destination_under_a_regular_file_refuses_for_its_parent(tmp_path):
    """The inside check skips an ancestor that is a file, and the destination check refuses.

    Catches the inside check treating ``NotADirectoryError`` as a failure to stat, which
    reports a failed check rather than the parent that is not a directory.
    """
    root, client = _simple(tmp_path)
    plain = tmp_path / "plain.txt"
    plain.write_text("x")
    client.calls.clear()

    with pytest.raises(RestoreRefused) as refused:
        _read(client, plain / "reading", root)

    assert str(refused.value) == f"{plain} does not exist, so nothing was restored"
    assert client.calls == []


def test_a_lake_root_spelled_with_a_tilde_still_refuses_a_destination_inside_it(
    tmp_path, monkeypatch
):
    """A direct caller passing ``~/lake`` gets the refusal a caller passing the full path gets.

    ``HOME`` points at the test's own directory. Catches ``lake_root`` left unexpanded, where
    ``~/lake`` names a directory under the working directory and the destination reads as
    outside it.
    """
    root, client = _simple(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    dest = root / "reading"
    client.calls.clear()

    with pytest.raises(RestoreRefused) as refused:
        _read(client, dest, "~/lake")

    assert str(refused.value) == (
        f"{dest} is inside lake_root {root}. A reading restore writes only outside the live "
        "lake, so nothing was restored. Name a directory elsewhere"
    )
    assert not dest.exists()
    assert client.calls == []


def _linked_work(tmp_path: Path, root: Path) -> Path:
    """A destination whose working directory is a link to a directory inside ``lake_root``."""
    inner = root / "inner"
    inner.mkdir()
    dest = tmp_path / "reading"
    dest.mkdir()
    (dest / WORK).symlink_to(inner)
    return dest


@pytest.mark.parametrize("command", ["reading", "restore"])
def test_a_working_directory_that_is_a_symbolic_link_refuses_and_writes_nothing(tmp_path, command):
    """A linked ``.marketlake-restoring`` would send every download wherever it points.

    Both commands share the check. Catches dropping it, where the downloads land in the
    directory inside the live lake that the link names.
    """
    root, client = _simple(tmp_path)
    dest = _linked_work(tmp_path, root)
    before = _files(root)
    client.calls.clear()

    with pytest.raises(RestoreRefused) as refused:
        if command == "reading":
            _read(client, dest, root)
        else:
            bucket.restore_lake(dest, TARGET, client=client)

    assert str(refused.value) == (
        f"{dest / WORK} is a symbolic link, and the files would land wherever it points, so "
        "nothing was restored. Remove it, since a restore makes its own working directory"
    )
    assert _files(root) == before
    assert os.listdir(root / "inner") == []
    assert client.calls == []


def _skip_as_root() -> None:
    if os.geteuid() == 0:
        pytest.skip("root ignores file modes, so chmod cannot make the local failure")


def test_a_destination_that_cannot_be_read_is_one_line_through_main(tmp_path, monkeypatch, capsys):
    """A destination nobody can list exits 2 with one line rather than a traceback.

    Catches the ``OSError`` from reading the destination escaping ``_destination_state``.
    """
    _skip_as_root()
    root, client = _simple(tmp_path)
    dest = tmp_path / "reading"
    dest.mkdir()
    dest.chmod(0)
    client.calls.clear()
    try:
        with pytest.raises(SystemExit) as exc:
            _main(tmp_path, root, client, monkeypatch, [str(dest), "--surface", "chains"])
    finally:
        dest.chmod(0o755)

    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert err.startswith(f"{LABEL}: reading {dest} failed (PermissionError: ")
    assert err.endswith(", so nothing was restored\n")
    assert err.count("\n") == 1
    assert client.calls == []
    assert os.listdir(dest) == []


def test_a_destination_that_cannot_be_read_refuses_the_restore_too(tmp_path):
    """Catches the same ``OSError`` escaping the ``restore`` command's call."""
    _skip_as_root()
    _root, client = _simple(tmp_path)
    dest = tmp_path / "restored"
    dest.mkdir()
    dest.chmod(0)
    client.calls.clear()
    try:
        with pytest.raises(RestoreRefused, match=r"^reading .* failed \(PermissionError: "):
            bucket.restore_lake(dest, TARGET, client=client)
    finally:
        dest.chmod(0o755)

    assert client.calls == []


# -- 4. the journal reserve ----------------------------------------------------------------


def _sizes(client: FakeS3) -> dict[str, int]:
    return {
        key.removeprefix("lake/"): len(versions[-1].body)
        for key, versions in client.objects.items()
    }


def _needed(client: FakeS3) -> int:
    """The planned bytes for SPY D2 to D3 on the simple bucket, written out here."""
    sizes = _sizes(client)
    planned = (_chains("SPY", D2), _chains("SPY", D3), _chains("SPY", D4), LEDGER)
    return sizes["manifest.jsonl"] + sum(sizes[rel] for rel in planned)


def _reserve(client: FakeS3) -> int:
    """13 times the busiest sealed day in the simple bucket, with the days written out here."""
    sizes = _sizes(client)
    days = {
        D1: [_chains("SPY", D1)],
        D2: [_chains("SPY", D2), _chains("QQQ", D2)],
        D3: [_chains("SPY", D3)],
        D4: [_chains("SPY", D4)],
    }
    return RESERVE_SESSIONS * max(sum(sizes[rel] for rel in rels) for rels in days.values())


def _huge_reserve(client: FakeS3, monkeypatch) -> int:
    """Raise the reserve past the floor, which the simple bucket's few KB never reach.

    The sessions are computed from the literal ``FLOOR`` so the reserve is more than twice the
    floor, and the reserve returned is that count times the busiest sealed day, which ``_reserve``
    reads off the bucket.
    """
    busiest = _reserve(client) // RESERVE_SESSIONS
    sessions = 2 * FLOOR // busiest + 1
    monkeypatch.setattr(runway, "JOURNAL_RESERVE_SESSIONS", sessions)
    reserve = sessions * busiest
    assert reserve > 2 * FLOOR
    return reserve


def _floor_line(dest: Path, root: Path, *, needed: int, free: int) -> str:
    """The floor refusal, written out here with the floor as a literal."""
    return (
        f"the restore needs {needed / 1_000_000:.1f} MB, and a filesystem lake_root {root} does "
        "not use must keep 1000.0 MB free beside it for the files a host writes during a "
        f"session, such as token.json. The filesystem holding {dest} has "
        f"{free / 1_000_000:.1f} MB free, {(FLOOR - (free - needed)) / 1_000_000:.1f} MB short "
        "of that floor, so nothing was restored. Free that much or use a larger filesystem"
    )


def _devices(lake_root: Path, *, same: bool):
    """A device check that puts every path on lake_root's device, or every other path off it."""

    def device_of(path: Path) -> int:
        return 1 if same or path == lake_root else 2

    return device_of


def test_the_reserve_applies_on_lake_root_s_filesystem(tmp_path):
    """Room for the plan and not the reserve refuses there, and the reserve's room restores.

    Catches dropping the device comparison so the reserve never applies. The simple bucket's
    reserve is a few KB, far under the floor, so the restore also catches the floor applying
    on lake_root's filesystem beside the reserve.
    """
    root, client = _simple(tmp_path)
    needed, reserve = _needed(client), _reserve(client)
    assert reserve > 0

    with pytest.raises(RestoreRefused) as refused:
        _read(
            client,
            tmp_path / "short",
            root,
            free=needed + reserve - 1,
            device_of=_devices(root, same=True),
        )
    summary = _read(
        client,
        tmp_path / "enough",
        root,
        free=needed + reserve,
        device_of=_devices(root, same=True),
    )

    message = str(refused.value)
    assert "short of the reserve" in message
    assert message.endswith(
        f"A directory on a filesystem other than lake_root {root}'s needs no reserve, only "
        "1000.0 MB free beside the plan"
    )
    assert "lake_volume_gib" not in message
    assert not (tmp_path / "short").exists()
    assert summary.restored is True


def test_on_lake_root_s_filesystem_the_floor_does_not_stand_in_for_the_reserve(
    tmp_path, monkeypatch
):
    """With a reserve larger than the floor, clearing the floor alone still refuses there.

    The previous test's reserve is a few KB, under the floor, so its restore catches the floor
    applying on lake_root's filesystem too. This one catches the reverse: the floor in place
    of the reserve there, or the device comparison dropped so the floor applies everywhere.
    """
    root, client = _simple(tmp_path)
    needed, reserve = _needed(client), _huge_reserve(client, monkeypatch)
    same = _devices(root, same=True)

    with pytest.raises(RestoreRefused) as refused:
        _read(client, tmp_path / "short", root, free=needed + FLOOR, device_of=same)
    summary = _read(client, tmp_path / "enough", root, free=needed + reserve, device_of=same)

    assert "short of the reserve" in str(refused.value)
    assert not (tmp_path / "short").exists()
    assert summary.restored is True


def test_the_reserve_does_not_apply_on_another_filesystem(tmp_path, monkeypatch):
    """The plan and exactly the floor free restores elsewhere, under a reserve twice the floor.

    Catches dropping the device comparison so the reserve always applies.
    """
    root, client = _simple(tmp_path)
    needed = _needed(client)
    _huge_reserve(client, monkeypatch)

    summary = _read(
        client,
        tmp_path / "reading",
        root,
        free=needed + FLOOR,
        device_of=_devices(root, same=False),
    )

    assert summary.restored is True


def test_another_filesystem_keeps_the_floor_free_after_the_plan(tmp_path):
    """One byte under the plan and the floor refuses with the floor's line, and exactly it restores.

    Catches a floor of 0 or off by one either way, a ``<=`` for the ``<``, and a floor check
    dropped so a read off the lake's filesystem only has to hold its plan.
    """
    root, client = _simple(tmp_path)
    needed = _needed(client)
    elsewhere = _devices(root, same=False)
    short = tmp_path / "short"
    client.calls.clear()

    with pytest.raises(RestoreRefused) as refused:
        _read(client, short, root, free=needed + FLOOR - 1, device_of=elsewhere)
    gets = _gets(client)
    summary = _read(client, tmp_path / "exact", root, free=needed + FLOOR, device_of=elsewhere)

    assert str(refused.value) == _floor_line(short, root, needed=needed, free=needed + FLOOR - 1)
    assert not short.exists()
    assert gets == ["lake/manifest.jsonl"]
    assert summary.restored is True


def test_the_plan_must_still_fit_on_another_filesystem(tmp_path):
    """Catches a device check that skips the free-space check along with the reserve."""
    root, client = _simple(tmp_path)
    needed = _needed(client)

    with pytest.raises(RestoreRefused, match="MB free, so nothing was restored"):
        _read(
            client,
            tmp_path / "reading",
            root,
            free=needed - 1,
            device_of=_devices(root, same=False),
        )


def test_every_number_in_the_floor_line_is_its_own(tmp_path):
    """A plan past a megabyte and a shortfall of a few hundred KB give four distinct figures.

    The simple bucket's plan is a few KB, so in the previous floor test the plan and the
    shortfall both print as 0.0 MB and the free space as 1000.0 MB, the floor's own figure.
    Here they print as 1.2, 1000.0, 1000.9 and 0.3 MB. Catches the shortfall computed as 0, as
    1, as ``free - floor`` or without the plan, and the line printing one figure in another's
    place: the shortfall or 0 for the plan, the floor, ``free - needed`` or ``floor + needed``
    for the free space, and the plan or 0 for the shortfall.
    """
    root, client = _simple(tmp_path)
    grown = _chains("SPY", D3)
    _record_in_bucket(client, grown, client.body(f"lake/{grown}") + b"\0" * 1_234_567)
    needed = _needed(client)
    assert needed > 1_000_000
    free = needed + FLOOR - 345_678
    short = tmp_path / "short"

    with pytest.raises(RestoreRefused) as refused:
        _read(client, short, root, free=free, device_of=_devices(root, same=False))

    assert str(refused.value) == _floor_line(short, root, needed=needed, free=free)
    assert ", 0.3 MB short of that floor" in str(refused.value)


@pytest.mark.parametrize(("under", "shown"), [(49_999, "0.0"), (50_000, "0.1")])
def test_the_floor_shortfall_is_exact_where_its_rounding_turns(tmp_path, under, shown):
    """A shortfall of 49,999 bytes prints as 0.0 MB and one of 50,000 as 0.1 MB.

    Catches the shortfall off by one byte either way, which no figure that rounds away from
    a 0.05 MB boundary can show.
    """
    root, client = _simple(tmp_path)
    needed = _needed(client)

    with pytest.raises(RestoreRefused) as refused:
        _read(
            client,
            tmp_path / "short",
            root,
            free=needed + FLOOR - under,
            device_of=_devices(root, same=False),
        )

    assert f", {shown} MB short of that floor" in str(refused.value)


def test_the_floor_is_read_from_runway(tmp_path, monkeypatch):
    """A floor of 5 MB moves the boundary, the floor line and the reserve line's floor clause.

    Catches ``restore_for_reading`` or ``_floor_refusal`` comparing against or printing a
    literal 1 GB rather than ``runway.OFF_LAKE_FREE_FLOOR_BYTES``, and the reserve line's
    floor clause written as the literal text 1000.0 MB. At the real floor each of these
    prints and decides exactly as the constant does, so only a changed floor shows them.
    """
    monkeypatch.setattr(runway, "OFF_LAKE_FREE_FLOOR_BYTES", 5_000_000)
    root, client = _simple(tmp_path)
    needed = _needed(client)
    elsewhere = _devices(root, same=False)

    with pytest.raises(RestoreRefused) as refused:
        _read(client, tmp_path / "short", root, free=needed + 5_000_000 - 1, device_of=elsewhere)
    summary = _read(client, tmp_path / "exact", root, free=needed + 5_000_000, device_of=elsewhere)
    monkeypatch.setattr(runway, "JOURNAL_RESERVE_SESSIONS", 10**9)
    with pytest.raises(RestoreRefused) as reserve:
        _read(client, tmp_path / "r", root, free=needed + 1, device_of=_devices(root, same=True))

    assert "must keep 5.0 MB free beside it" in str(refused.value)
    assert summary.restored is True
    assert str(reserve.value).endswith("only 5.0 MB free beside the plan")


def test_a_lake_root_that_cannot_be_statted_keeps_the_reserve(tmp_path):
    """With nothing to prove the filesystems apart, the reserve applies.

    Catches reading an unstattable ``lake_root`` as another filesystem.
    """
    root, client = _simple(tmp_path)
    needed = _needed(client)

    def device_of(path: Path) -> int:
        if path == root:
            raise PermissionError(13, "Permission denied")
        return 2

    with pytest.raises(RestoreRefused, match="short of the reserve"):
        _read(client, tmp_path / "reading", root, free=needed, device_of=device_of)


@pytest.mark.parametrize("which", ["device", "free"])
def test_a_stat_that_fails_on_the_destination_is_one_line(tmp_path, which):
    """Catches a device check or a free-space read whose ``OSError`` escapes as a traceback."""
    root, client = _simple(tmp_path)

    def broken(path: Path) -> int:
        raise PermissionError(13, "Permission denied")

    kwargs = {"device_of": broken} if which == "device" else {"free_space": broken}
    with pytest.raises(RestoreRefused) as refused:
        restore_for_reading(
            tmp_path / "reading",
            TARGET,
            client=client,
            lake_root=root,
            surface="chains",
            ticker="SPY",
            first=D2,
            last=D3,
            **{"free_space": lambda path: PLENTY, **kwargs},
        )

    assert "failed (PermissionError: Permission denied), so nothing was restored" in str(
        refused.value
    )
    assert not (tmp_path / "reading").exists()


def test_the_real_device_check_puts_a_sibling_directory_on_lake_root_s_filesystem(tmp_path):
    """The default device check reads ``st_dev``, so a sibling of the lake pays the reserve."""
    root, client = _simple(tmp_path)
    needed = _needed(client)

    with pytest.raises(RestoreRefused, match="short of the reserve"):
        _read(client, tmp_path / "reading", root, free=needed)


def test_an_existing_empty_destination_is_where_free_space_and_the_device_are_read(tmp_path):
    """A fresh mount point is its own filesystem, so both checks ask about it, not its parent.

    The parent has no space and shares ``lake_root``'s device, so asking it refuses. Catches
    reading the parent, which on a mount point measures the wrong filesystem.
    """
    root, client = _simple(tmp_path)
    dest = tmp_path / "mount"
    dest.mkdir()
    free_asked: list[Path] = []
    device_asked: list[Path] = []

    def free_space(path: Path) -> int:
        free_asked.append(path)
        return PLENTY if path == dest else 0

    def device_of(path: Path) -> int:
        device_asked.append(path)
        return 2 if path == dest else 1

    summary = restore_for_reading(
        dest,
        TARGET,
        client=client,
        lake_root=root,
        surface="chains",
        ticker="SPY",
        first=D2,
        last=D3,
        free_space=free_space,
        device_of=device_of,
    )

    assert summary.restored is True
    assert free_asked == [dest]
    assert device_asked == [dest, root]


def test_a_working_file_larger_than_its_listed_size_counts_toward_the_space_needed(tmp_path):
    """A working copy of D2 that grew past its listed size will be downloaded again.

    A first run fails on D3, so D2, D4 and the ledger wait verified in the working
    directory. Then D2's copy grows, and room for all but one of the bytes still to download
    refuses. Catches counting a file as done when it is at least its listed size rather than
    exactly it, which leaves D2 out of the space needed.
    """
    root, client = _simple(tmp_path)
    dest = tmp_path / "reading"
    key = f"lake/{_chains('SPY', D3)}"
    good = client.body(key)
    client.store(key, good + b"rot")
    assert _read(client, dest, root).restored is False
    client.store(key, good)
    grown = dest / WORK / _chains("SPY", D2)
    grown.write_bytes(grown.read_bytes() + b"extra")
    listed = len(client.body(f"lake/{_chains('SPY', D2)}"))
    needed = len(client.body("lake/manifest.jsonl")) + len(good) + listed
    elsewhere = _devices(root, same=False)

    with pytest.raises(RestoreRefused, match="MB short of that floor"):
        _read(client, dest, root, free=needed + FLOOR - 1, device_of=elsewhere)

    assert _read(client, dest, root, free=needed + FLOOR, device_of=elsewhere).restored is True


# -- 5. the marker, and refusals before the work ------------------------------------------


def _fail_the_move(monkeypatch) -> None:
    def refuse(work: Path, dest: Path) -> None:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(bucket, "_finish", refuse)


def _waiting(tmp_path: Path, monkeypatch, run) -> None:
    """Run ``run`` with the move failing, so the working directory waits verified."""
    with monkeypatch.context() as patch:
        _fail_the_move(patch)
        with pytest.raises(RestoreRefused, match="every file verified and moving them"):
            run()


def test_a_waiting_reading_restore_refuses_another_range_and_a_restore(tmp_path, monkeypatch):
    """A verified range moves in only for the run that names it.

    Catches dropping the marker's mode comparison or its selection comparison.
    """
    root, client = _simple(tmp_path)
    dest = tmp_path / "reading"
    _waiting(tmp_path, monkeypatch, lambda: _read(client, dest, root, first=D2, last=D2))
    client.calls.clear()

    with pytest.raises(RestoreRefused) as other_range:
        _read(client, dest, root, first=D3, last=D3)
    with pytest.raises(RestoreRefused) as whole:
        bucket.restore_lake(dest, TARGET, client=client)

    held = f"a verified reading restore of chains partitions for SPY from {D2} to {D2}"
    assert held in str(other_range.value)
    assert f"reading restore of chains partitions for SPY from {D3} to {D3}" in str(
        other_range.value
    )
    assert held in str(whole.value) and "this run is a restore of a lake" in str(whole.value)
    assert sorted(os.listdir(dest)) == [WORK]
    assert client.calls == []

    summary = _read(client, dest, root, first=D2, last=D2)

    assert summary.finished_move is True
    assert _files(dest) == _bucket_files(
        client, (_chains("SPY", D2), _chains("SPY", D3), LEDGER, "manifest.jsonl")
    )


def test_a_waiting_restore_refuses_a_reading_restore(tmp_path, monkeypatch):
    """A verified whole-lake restore moves in only for ``restore``.

    Catches a reading restore that finishes any verified directory it finds.
    """
    root, client = _simple(tmp_path)
    dest = tmp_path / "restored"
    _waiting(tmp_path, monkeypatch, lambda: bucket.restore_lake(dest, TARGET, client=client))

    with pytest.raises(RestoreRefused, match="holds a verified restore of a lake"):
        _read(client, dest, root)
    assert sorted(os.listdir(dest)) == [WORK]

    assert bucket.restore_lake(dest, TARGET, client=client).finished_move is True


def test_an_unreadable_marker_on_a_waiting_reading_restore_is_named_as_unreadable(
    tmp_path, monkeypatch
):
    """A verified marker torn while its reading restore waited refuses for the marker itself.

    Catches reading the torn marker as one with no mode, which refuses it as another
    command's verified ``restore of a lake`` and names the wrong command to finish it.
    """
    root, client = _simple(tmp_path)
    dest = tmp_path / "reading"
    _waiting(tmp_path, monkeypatch, lambda: _read(client, dest, root))
    (dest / WORK / ".marketlake-verified").write_text("{torn")

    with pytest.raises(RestoreRefused) as refused:
        _read(client, dest, root)

    assert str(refused.value) == (
        f"{dest / WORK} no longer holds .marketlake-verified as it verified, so nothing was "
        f"moved. Delete {dest / WORK} and run the same command again"
    )
    assert sorted(os.listdir(dest)) == [WORK]


def test_a_marker_with_no_mode_reads_as_a_restore(tmp_path, monkeypatch):
    """A marker written before reading restores existed finishes only as a ``restore``."""
    root, client = _simple(tmp_path)
    dest = tmp_path / "restored"
    _waiting(tmp_path, monkeypatch, lambda: bucket.restore_lake(dest, TARGET, client=client))
    marker = dest / WORK / ".marketlake-verified"
    record = json.loads(marker.read_text())
    marker.write_text(json.dumps({"files": record["files"]}) + "\n")

    with pytest.raises(RestoreRefused, match="holds a verified restore of a lake"):
        _read(client, dest, root)

    assert bucket.restore_lake(dest, TARGET, client=client).finished_move is True


def test_the_mode_is_compared_even_when_the_selections_agree(tmp_path, monkeypatch):
    """A reading marker whose range was lost still refuses a ``restore``.

    No run writes that marker, since a reading restore always records its range and a
    ``restore`` never does, so the selection alone tells the two apart on every marker the code
    writes. This edits one by hand so that only the mode differs. Catches dropping the mode
    comparison.
    """
    root, client = _simple(tmp_path)
    dest = tmp_path / "reading"
    _waiting(tmp_path, monkeypatch, lambda: _read(client, dest, root))
    marker = dest / WORK / ".marketlake-verified"
    record = json.loads(marker.read_text())
    marker.write_text(json.dumps({**record, "selection": None}) + "\n")

    with pytest.raises(RestoreRefused, match="holds a verified run of 'restore-for-reading'"):
        bucket.restore_lake(dest, TARGET, client=client)

    assert sorted(os.listdir(dest)) == [WORK]


def test_a_kill_between_the_two_marker_deletes_finishes_its_own_mode_s_move(tmp_path, monkeypatch):
    """Only the verified marker is left, which ``_finish`` deleting the restore marker first
    leaves. A reading restore finishes it, and a ``restore`` refuses it.
    """
    root, client = _simple(tmp_path)
    dest = tmp_path / "reading"

    def killed(work: Path, dest_: Path) -> None:
        for name in sorted(os.listdir(work)):
            if name not in (".marketlake-restore", ".marketlake-verified", "manifest.jsonl"):
                os.rename(work / name, dest_ / name)
        os.rename(work / "manifest.jsonl", dest_ / "manifest.jsonl")
        (work / ".marketlake-restore").unlink()
        raise OSError(4, "Interrupted system call")

    with monkeypatch.context() as patch:
        patch.setattr(bucket, "_finish", killed)
        with pytest.raises(RestoreRefused):
            _read(client, dest, root)
    assert sorted(os.listdir(dest / WORK)) == [".marketlake-verified"]

    with pytest.raises(RestoreRefused, match="holds a verified reading restore"):
        bucket.restore_lake(dest, TARGET, client=client)
    summary = _read(client, dest, root)

    assert summary.finished_move is True
    assert not (dest / WORK).exists()
    assert "manifest.jsonl" in os.listdir(dest)


def test_a_rerun_with_the_same_range_resumes_after_a_failed_file(tmp_path):
    """Catches a resume that downloads again, or a download branch refusing its own files."""
    root, client = _simple(tmp_path)
    dest = tmp_path / "reading"
    good = client.body(f"lake/{_chains('SPY', D3)}")
    client.store(f"lake/{_chains('SPY', D3)}", good + b"rot")

    first = _read(client, dest, root)
    assert first.restored is False
    assert first.failures == [(_chains("SPY", D3), "does not match its SHA-256")]
    client.store(f"lake/{_chains('SPY', D3)}", good)

    second = _read(client, dest, root)

    assert second.restored is True
    assert second.downloaded == 1
    assert second.resumed == 3


def test_a_corrected_range_after_a_failed_run_prunes_what_it_no_longer_plans(tmp_path):
    """Every ticker failed on QQQ, and SPY alone then restores without QQQ's leftovers."""
    root, client = _simple(tmp_path)
    dest = tmp_path / "reading"
    client.store(f"lake/{_chains('QQQ', D2)}", b"rot")

    first = _read(client, dest, root, ticker=None, first=D2, last=D2)
    assert first.restored is False

    second = _read(client, dest, root, ticker="SPY", first=D2, last=D2)

    assert second.restored is True
    assert _files(dest) == _bucket_files(
        client, (_chains("SPY", D2), _chains("SPY", D3), LEDGER, "manifest.jsonl")
    )


@pytest.mark.parametrize(
    ("args", "says"),
    [
        pytest.param(
            ["--surface", "bars"],
            "the reading restore takes chains or quotes partitions, not 'bars', so nothing was "
            "restored. Restore any other file by hand from the bucket",
            id="surface",
        ),
        pytest.param(
            ["--surface", "chains", "--from", "2026-08-26", "--to", "2026-08-25"],
            "the range ends on 2026-08-25, before it starts on 2026-08-26, so nothing was restored",
            id="reversed",
        ),
    ],
)
def test_a_bad_surface_or_a_reversed_range_refuses_before_any_request(
    tmp_path, monkeypatch, capsys, args, says
):
    """Catches either refusal moved after the bucket's manifest is read."""
    root, client = _simple(tmp_path)
    client.calls.clear()
    dest = tmp_path / "reading"

    with pytest.raises(SystemExit) as exc:
        _main(tmp_path, root, client, monkeypatch, [str(dest), *args])

    assert exc.value.code == 2
    assert capsys.readouterr().err == f"{LABEL}: {says}\n"
    assert client.calls == []
    assert not dest.exists()


def test_a_range_selecting_nothing_refuses_after_the_manifest_and_creates_nothing(
    tmp_path, monkeypatch, capsys
):
    """Catches the empty selection decided after the next partition, bars or support files
    join the plan, which would download them into a new working directory.
    """
    root, client = _simple(tmp_path)
    client.calls.clear()
    dest = tmp_path / "reading"
    argv = [str(dest), "--surface", "chains", "--from", "2026-08-21", "--to", "2026-08-21"]

    with pytest.raises(SystemExit) as exc:
        _main(tmp_path, root, client, monkeypatch, argv, extra=False)

    assert exc.value.code == 2
    assert capsys.readouterr().err == (
        f"{LABEL}: the manifest records no chains partitions for SPY from 2026-08-21 to "
        "2026-08-21, so nothing was restored. Check the surface, the ticker and the dates\n"
    )
    assert _gets(client) == ["lake/manifest.jsonl"]
    assert not dest.exists()


def test_an_every_ticker_range_selecting_nothing_says_every_ticker(tmp_path):
    """Catches the range's words printing ``None`` when no ticker was given."""
    root, client = _simple(tmp_path)

    with pytest.raises(RestoreRefused) as refused:
        _read(
            client, tmp_path / "reading", root, ticker=None, first="2026-08-21", last="2026-08-21"
        )

    assert str(refused.value) == (
        "the manifest records no chains partitions for every ticker from 2026-08-21 to "
        "2026-08-21, so nothing was restored. Check the surface, the ticker and the dates"
    )


# -- 6. the shadow host --------------------------------------------------------------------


def _main(
    tmp_path: Path,
    lake_root: Path,
    client: FakeS3,
    monkeypatch,
    args: list[str],
    *,
    role: str | None = None,
    extra: bool = True,
    ticker: bool = True,
    window: str | None = None,
) -> int:
    """``main`` with the client patched in, as ``test_bucket_rebuild.py`` drives it.

    ``extra`` fills in the D2 to D3 range, and ``ticker`` fills in SPY, when the arguments
    leave them out. ``window`` writes ``lake_window_sessions`` with that text.
    """
    config = write_config(
        tmp_path,
        lake_root,
        role=role,
        bucket_access_key_id="AKIDCONFIG",
        bucket_secret_access_key="secret-bucket-key",
        bucket_region="us-east-2",
    )
    if window is not None:
        config.write_text(config.read_text() + f"lake_window_sessions: {window}\n")
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    argv = [LABEL, *args]
    if ticker and "--ticker" not in argv:
        argv += ["--ticker", "SPY"]
    if extra and "--from" not in argv:
        argv += ["--from", D2.isoformat(), "--to", D3.isoformat()]
    argv += ["--config", str(config), "--target", "s3://lake-backup/lake"]
    return bucket.main(argv, clock=ManualClock(MONDAY_19), calendar=CALENDAR)


@pytest.mark.parametrize("role", ["shadow", None], ids=["shadow", "primary"])
def test_the_command_runs_on_either_role(tmp_path, monkeypatch, capsys, role):
    """Catches leaving the command out of the shadow host's allowed commands."""
    root, client = _simple(tmp_path)
    dest = tmp_path / "reading"

    code = _main(tmp_path, root, client, monkeypatch, [str(dest), "--surface", "chains"], role=role)

    assert code == 0
    out = capsys.readouterr().out.splitlines()
    assert len(out) == 1
    assert out[0].startswith(f"{LABEL}: restored for reading 2 partition(s) in the range (SPY 2)")
    assert out[0].endswith(
        f"Read it with lake_root={dest}, and never make it a daemon's or a "
        "job's lake_root, since its manifest records partitions it does not hold"
    )
    assert _files(dest) == _bucket_files(
        client,
        (_chains("SPY", D2), _chains("SPY", D3), _chains("SPY", D4), LEDGER, "manifest.jsonl"),
    )


def test_the_shadow_host_still_refuses_the_range_restore(tmp_path, monkeypatch, capsys):
    """The new command joins the shadow's allowed list without widening it to its neighbour."""
    root, client = _simple(tmp_path)
    config = write_config(
        tmp_path,
        root,
        role="shadow",
        bucket_access_key_id="AKIDCONFIG",
        bucket_secret_access_key="secret-bucket-key",
        bucket_region="us-east-2",
    )
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    argv = ["restore-range", "--surface", "chains", "--from", "2026-08-25", "--to", "2026-08-25"]

    assert bucket.main([*argv, "--config", str(config)], clock=ManualClock(MONDAY_19)) == 2
    assert "Run it on the primary" in capsys.readouterr().err


def test_a_finished_move_has_its_own_line_through_main(tmp_path, monkeypatch, capsys):
    """Catches the reading restore's finished move printed with the restore's lake wording."""
    root, client = _simple(tmp_path)
    dest = tmp_path / "reading"
    _waiting(tmp_path, monkeypatch, lambda: _read(client, dest, root))

    code = _main(tmp_path, root, client, monkeypatch, [str(dest), "--surface", "chains"])

    assert code == 0
    assert capsys.readouterr().out == (
        f"{LABEL}: finished moving a reading restore that had already verified every file into "
        f"{dest}. Read it with lake_root={dest}, and never make it a daemon's or a job's "
        "lake_root, since its manifest records partitions it does not hold\n"
    )


# -- 7. the command's arguments and lines -------------------------------------------------


def test_main_refuses_a_destination_inside_the_configured_lake_root(tmp_path, monkeypatch, capsys):
    """Catches ``main`` handing the reading restore anything but ``config.lake_root``.

    With any other root, the destination under the live lake reads as outside and is filled.
    """
    root, client = _simple(tmp_path)
    dest = root / "reading"
    client.calls.clear()

    with pytest.raises(SystemExit) as exc:
        _main(tmp_path, root, client, monkeypatch, [str(dest), "--surface", "chains"])

    assert exc.value.code == 2
    assert capsys.readouterr().err == (
        f"{LABEL}: {dest} is inside lake_root {root}. A reading restore writes only outside the "
        "live lake, so nothing was restored. Name a directory elsewhere\n"
    )
    assert not dest.exists()
    assert client.calls == []


def _main_with_space(tmp_path, root, client, monkeypatch, dest: Path, *, free: int, same: bool):
    """``_main`` with the reading restore's free space and device check injected.

    ``main`` passes neither, so the wrapper hands them to the real ``restore_for_reading``,
    the one this module imported, never one an earlier call patched in.
    """

    def injected(*args, **kwargs):
        return restore_for_reading(
            *args, **kwargs, free_space=lambda path: free, device_of=_devices(root, same=same)
        )

    monkeypatch.setattr(bucket, "restore_for_reading", injected)
    return _main(tmp_path, root, client, monkeypatch, [str(dest), "--surface", "chains"])


def test_main_refuses_a_read_that_would_leave_less_than_the_floor_elsewhere(
    tmp_path, monkeypatch, capsys
):
    """Through ``main``, one byte under the floor exits 2 with its line, and the floor exits 0.

    Catches the floor dropped, off by one, or compared with ``<=``, on the command's own path.
    """
    root, client = _simple(tmp_path)
    needed = _needed(client)
    short = tmp_path / "short"

    with pytest.raises(SystemExit) as exc:
        _main_with_space(
            tmp_path, root, client, monkeypatch, short, free=needed + FLOOR - 1, same=False
        )
    err = capsys.readouterr().err
    code = _main_with_space(
        tmp_path, root, client, monkeypatch, tmp_path / "exact", free=needed + FLOOR, same=False
    )

    assert exc.value.code == 2
    assert err == f"{LABEL}: {_floor_line(short, root, needed=needed, free=needed + FLOOR - 1)}\n"
    assert not short.exists()
    assert code == 0


def test_main_keeps_the_reserve_rather_than_the_floor_on_lake_root_s_filesystem(
    tmp_path, monkeypatch, capsys
):
    """Through ``main``, the floor alone refuses on lake_root's filesystem, and the reserve passes.

    Catches the floor in place of the reserve on the command's own path.
    """
    root, client = _simple(tmp_path)
    needed, reserve = _needed(client), _huge_reserve(client, monkeypatch)
    short = tmp_path / "short"

    with pytest.raises(SystemExit) as exc:
        _main_with_space(tmp_path, root, client, monkeypatch, short, free=needed + FLOOR, same=True)
    err = capsys.readouterr().err
    code = _main_with_space(
        tmp_path, root, client, monkeypatch, tmp_path / "enough", free=needed + reserve, same=True
    )

    assert exc.value.code == 2
    assert err.startswith(f"{LABEL}: the restore needs {needed / 1_000_000:.1f} MB")
    assert "short of the reserve" in err
    assert "short of that floor" not in err
    assert not short.exists()
    assert code == 0


def test_main_without_a_ticker_takes_every_ticker(tmp_path, monkeypatch, capsys):
    """Leaving ``--ticker`` out takes QQQ's D2 beside SPY's.

    Catches the parser defaulting ``--ticker`` to one ticker rather than to every ticker.
    """
    root, client = _simple(tmp_path)
    dest = tmp_path / "reading"
    args = [str(dest), "--surface", "chains", "--from", D2.isoformat(), "--to", D2.isoformat()]

    code = _main(tmp_path, root, client, monkeypatch, args, ticker=False)

    assert code == 0
    assert capsys.readouterr().out.startswith(
        f"{LABEL}: restored for reading 2 partition(s) in the range (QQQ 1, SPY 1)"
    )
    assert (dest / _chains("QQQ", D2)).is_file()


def test_main_without_a_surface_is_a_usage_error(tmp_path, monkeypatch, capsys):
    """Catches the parser defaulting ``--surface`` rather than requiring it."""
    root, client = _simple(tmp_path)
    dest = tmp_path / "reading"
    client.calls.clear()

    with pytest.raises(SystemExit) as exc:
        _main(tmp_path, root, client, monkeypatch, [str(dest)])

    assert exc.value.code == 2
    assert "the following arguments are required: --surface" in capsys.readouterr().err
    assert client.calls == []
    assert not dest.exists()


def test_an_unreachable_bucket_says_a_rerun_resumes(tmp_path, monkeypatch, capsys):
    """One line that says the destination is untouched and a re-run resumes.

    Catches the reading restore left out of the commands whose unreachable line says so.
    """
    root, client = _simple(tmp_path)
    client.fail_with = unreachable()
    dest = tmp_path / "reading"

    with pytest.raises(SystemExit) as exc:
        _main(tmp_path, root, client, monkeypatch, [str(dest), "--surface", "chains"])

    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert err.startswith(f"{LABEL}: ")
    assert err.endswith(
        ". The destination is untouched, and a re-run resumes in the working directory\n"
    )
    assert err.count("\n") == 1
    assert not dest.exists()


def test_a_malformed_window_key_does_not_block_a_reading_restore(tmp_path, monkeypatch, capsys):
    """``lake_window_sessions: many`` refuses a ``restore`` and never a reading restore.

    Catches the reading restore judging the window key, which it never uses.
    """
    root, client = _simple(tmp_path)
    dest = tmp_path / "reading"

    code = _main(
        tmp_path, root, client, monkeypatch, [str(dest), "--surface", "chains"], window="many"
    )

    assert code == 0
    assert "lake_window_sessions" not in capsys.readouterr().err
    assert (dest / "manifest.jsonl").is_file()


def test_the_summary_counts_decimal_megabytes(tmp_path):
    """1,500,000 bytes print as 1.5 MB. Catches dividing by 1,048,576, which prints 1.4."""
    summary = bucket.ReadingRestoreSummary(
        target="s3://lake-backup/lake",
        dest=tmp_path,
        work=tmp_path / WORK,
        downloaded=1,
        downloaded_bytes=1_500_000,
    )

    assert "downloaded 1 (1.5 MB)" in summary.render()


def test_the_summary_lists_the_tickers_in_sorted_order(tmp_path):
    """``BRK`` prints before ``BRK.B``, though the plan meets ``BRK.B`` first.

    ``ticker=BRK.B/`` sorts before ``ticker=BRK/``, since ``.`` sorts before ``/``. Catches
    printing the tickers in the order the plan met them.
    """
    lake = FixtureLake(tmp_path / "lake")
    lake.with_chains("BRK", D2, sample_chains_table()).with_chains("BRK.B", D2)
    lake.with_reference("schema_versions", chain._ledger_table())
    root = lake.build()
    client = _upload(root)

    summary = _read(client, tmp_path / "reading", root, ticker=None, first=D2, last=D2)

    assert list(summary.range_by_ticker) == ["BRK.B", "BRK"]
    assert "in the range (BRK 1, BRK.B 1)" in summary.render()
