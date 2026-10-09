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
4. The journal reserve applies only when the destination shares ``lake_root``'s filesystem.
5. The verified marker records the mode and the range, so a waiting directory moves in only
   for the run that made it. Refusals on the arguments and on an empty range come first.
6. The command runs on a shadow host.

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

from lake import bucket
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
from tests.support.bucket import FakeS3
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
    D5. A second ticker and a quotes partition inside the range's dates. ``1m`` and ``1d`` bars
    inside and outside the range, and a second ticker's. The actions ledger, quarantine, the
    three reference tables, a report, a manifested journal segment and an unmanifested file
    under ``reference/``. Then SPY's D2 is trimmed and the nightly upload carries the trim.
    """
    candles = sample_chains_table()
    lake = FixtureLake(tmp_path / "lake")
    for day in (D1, D2, D3, D4, D5):
        lake.with_chains("SPY", day)
    lake.with_chains("QQQ", D2).with_quotes("SPY", D2)
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
    segment, the unrecorded file, the trimmed ledger and every other ticker and day.
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
    """Leaving out ``--ticker`` takes QQQ's partition and bars too, and QQQ has no next one.

    Catches a selection that ignores ``ticker=None`` and a bars plan built from the argument
    rather than from the tickers the range selected.
    """
    lake = _inventory(tmp_path)
    dest = tmp_path / "reading"

    summary = _read(lake.client, dest, lake.root, ticker=None)

    extra = (_chains("QQQ", D2), _bars("QQQ", "1d", D2))
    assert _files(dest) == _bucket_files(lake.client, (*PLANNED, *extra))
    assert summary.range_by_ticker == {"QQQ": 1, "SPY": 2}


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


# -- 1. each view reads the same answer --------------------------------------------------


def test_load_chain_reads_the_range_as_the_source_lake_does_and_excludes_the_quarantined(
    tmp_path,
):
    """``load_chain`` on both sessions of the range, one of them quarantined.

    Catches leaving out ``quarantine.jsonl``, where the reading directory serves the flagged
    partition with no error, and leaving out ``reference/``, where the schema-version ledger is
    missing and the read raises ``PartialRead``.
    """
    source = chain._lake(
        FixtureLake(tmp_path / "lake"),
        quarantine=[{"partition": chain.FULL_PARTITION, "verdict": "delayed_feed"}],
    )
    client = _upload(source)
    dest = tmp_path / "reading"

    _read(client, dest, source, first=chain.FULL_DAY, last=chain.HALF_DAY)

    for root in (source, dest):
        with pytest.raises(PartitionQuarantined):
            load_chain("SPY", chain.FULL_DAY, lake_root=root)
    assert load_chain("SPY", chain.HALF_DAY, lake_root=dest).equals(
        load_chain("SPY", chain.HALF_DAY, lake_root=source)
    )
    assert load_chain("SPY", chain.FULL_DAY, lake_root=dest, include_quarantined=True).equals(
        load_chain("SPY", chain.FULL_DAY, lake_root=source, include_quarantined=True)
    )


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
    """``load_contract_life`` with ``end`` at the range's last day.

    Catches leaving out ``reference/``, where the master that threads the contract is missing.
    """
    source = life._lake(FixtureLake(tmp_path / "lake"), master=life._master())
    client = _upload(source)
    dest = tmp_path / "reading"
    last = life.SEALED[2]

    _read(client, dest, source, first=life.SEALED[0], last=last)

    answer = load_contract_life(life.OLD, end=last, lake_root=dest)
    assert answer.num_rows == 3
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
    assert len(err) == 3


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


def _devices(lake_root: Path, *, same: bool):
    """A device check that puts every path on lake_root's device, or every other path off it."""

    def device_of(path: Path) -> int:
        return 1 if same or path == lake_root else 2

    return device_of


def test_the_reserve_applies_on_lake_root_s_filesystem(tmp_path):
    """Room for the plan and not the reserve refuses there, and the reserve's room restores.

    Catches dropping the device comparison so the reserve never applies.
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
        f"A directory on a filesystem other than lake_root {root}'s needs no reserve"
    )
    assert "lake_volume_gib" not in message
    assert not (tmp_path / "short").exists()
    assert summary.restored is True


def test_the_reserve_does_not_apply_on_another_filesystem(tmp_path):
    """Exactly the plan's bytes free restores on a filesystem that is not lake_root's.

    Catches dropping the device comparison so the reserve always applies.
    """
    root, client = _simple(tmp_path)
    needed = _needed(client)

    summary = _read(
        client, tmp_path / "reading", root, free=needed, device_of=_devices(root, same=False)
    )

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
) -> int:
    """``main`` with the client patched in, as ``test_bucket_rebuild.py`` drives it.

    ``extra`` fills in the ticker and the D2 to D3 range when the arguments leave them out.
    """
    config = write_config(
        tmp_path,
        lake_root,
        role=role,
        bucket_access_key_id="AKIDCONFIG",
        bucket_secret_access_key="secret-bucket-key",
        bucket_region="us-east-2",
    )
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    argv = [LABEL, *args]
    if "--ticker" not in argv:
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
