"""The trim, called directly over a small window (marketlake #787).

``lake.trim.trim`` drops a chains partition only when all seven clauses hold, writes its trim
line before it unlinks, recovers a crash between the two by re-running the whole selection, and
never raises. The window's floor is at least 22 sessions, so these tests call the trim with a
window of 3 over two weeks of tiny partitions, each stored in the fake bucket, and the floor and
gate tests run at the ``compact`` level in ``test_compaction_trim.py``.

The lake below holds SPY and QQQ on every weekday from 2026-08-17 to 2026-08-28. Tonight is
Friday 2026-08-28 at 16:30, the checkpoint is Thursday's, and its cutoffs sit at the window edge,
Tuesday 2026-08-25. A window of 3 keeps Wednesday, Thursday and Friday, so seven sessions per
ticker are candidates.

Post-crash states are built by hand rather than by injecting a crash.
"""

from __future__ import annotations

import errno
import json
import os
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from lake import trim as trim_module
from lake import trimmed
from lake.bucket import UploadSummary
from lake.calendar import MARKET_TZ
from lake.config import BucketTarget
from lake.lock import lake_lock
from lake.manifest import RowCountRegression, latest_entries, read_manifest, sha256_file
from lake.paths import LakePaths
from lake.split_checkpoint import (
    Checkpoint,
    CheckpointEntry,
    _to_table,
    checkpoint_path,
    starting_state,
    write_checkpoint,
)
from lake.trim import LOCAL_GOOD, LOCAL_ROTTED, TrimResult, rot_page_body, trim
from tests.component.test_compaction import _chains, _snap
from tests.support.bucket import FakeS3, client_error, unreachable
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.lake import FixtureLake

TARGET = BucketTarget(bucket="lake-backup", prefix="lake")
FIRST_MONDAY = date(2026, 8, 17)
SECOND_MONDAY = date(2026, 8, 24)
SESSIONS = [
    FIRST_MONDAY + timedelta(days=offset) for offset in range(12) if offset % 7 < 5
]  # 2026-08-17 to 2026-08-28, ten weekdays
TONIGHT = date(2026, 8, 28)
YESTERDAY = date(2026, 8, 27)
EDGE = date(2026, 8, 25)
WINDOW = 3
TICKERS = ("QQQ", "SPY")
CALENDAR = weekday_sessions(FIRST_MONDAY, SECOND_MONDAY)


def _et(day: date, hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=MARKET_TZ)


def _rel(ticker: str, day: date) -> str:
    return f"chains/ticker={ticker}/date={day.isoformat()}.parquet"


def _state(ticker: str, cutoff: date):
    start = starting_state(ticker)
    return start if cutoff == date.min else replace(start, cutoff=cutoff, last_day=cutoff)


def _checkpoint(root: Path, session_day: date, cutoffs: dict[str, date]) -> Checkpoint:
    checkpoint = Checkpoint(
        session_day=session_day,
        entries=tuple(
            CheckpointEntry(state=_state(ticker, cutoff), mappings=())
            for ticker, cutoff in cutoffs.items()
        ),
    )
    write_checkpoint(root, checkpoint, recorded_at=_et(session_day, 18, 30))
    return checkpoint


def _build(
    root: Path,
    *,
    days=SESSIONS,
    tickers=TICKERS,
    quarantine: tuple[str, ...] = (),
    session_day: date | None = YESTERDAY,
    cutoffs: dict[str, date] | None = None,
) -> FakeS3:
    """A lake of tiny chains partitions, each stored in the bucket, and a checkpoint."""
    lake = FixtureLake(root)
    for day in days:
        for ticker in tickers:
            lake.with_partition(
                "chains", ticker, day, _chains(1, snap_ts=_snap(day, 0), ticker=ticker)
            )
    for rel in quarantine:
        lake.with_quarantine({"partition": rel, "check": "test", "verdict": "held"})
    lake.build()
    if session_day is not None:
        _checkpoint(
            root,
            session_day,
            cutoffs if cutoffs is not None else dict.fromkeys(tickers, EDGE),
        )
    client = FakeS3()
    for day in days:
        for ticker in tickers:
            rel = _rel(ticker, day)
            client.store(TARGET.key(rel), (root / rel).read_bytes())
    return client


def _upload(root: Path, *, watermark: int | None = None, deadline: datetime | None = None):
    return UploadSummary(
        target=str(TARGET),
        deadline=deadline if deadline is not None else _et(TONIGHT, 17, 45),
        watermark=watermark if watermark is not None else len(read_manifest(root)),
    )


def _trim(
    root: Path,
    client: FakeS3,
    *,
    window: int = WINDOW,
    clock: ManualClock | None = None,
    upload: UploadSummary | None = None,
    calendar=CALENDAR,
) -> TrimResult:
    with lake_lock(root):
        return trim(
            root,
            window=window,
            client=client,
            target=TARGET,
            upload=upload if upload is not None else _upload(root),
            clock=clock if clock is not None else ManualClock(_et(TONIGHT, 16, 30)),
            calendar=calendar,
        )


def _expected(edge: date = EDGE, tickers=TICKERS) -> list[str]:
    """Every partition at or before ``edge``, oldest first, the order the trim takes them."""
    return [_rel(ticker, day) for day in SESSIONS if day <= edge for ticker in sorted(tickers)]


def _gets(client: FakeS3) -> list[str]:
    return [TARGET.rel(kwargs["Key"]) for name, kwargs in client.calls if name == "get_object"]


def _lines(root: Path) -> list[dict]:
    return trimmed.read_trimmed(root)


def _absent_on_purpose(root: Path, rel: str) -> bool:
    return not (root / rel).exists() and trimmed.is_designed_absence(
        rel, latest_entries(root), trimmed.latest_trimmed(root)
    )


# -- a passing partition is trimmed, oldest first --------------------------------------


def test_every_partition_past_the_window_is_trimmed_and_the_window_stays(lake_root):
    client = _build(lake_root)

    result = _trim(lake_root, client)

    expected = _expected()
    assert list(result.trimmed) == expected
    assert result.refused is None and result.stopped is None and result.deadline is None
    assert result.edge == EDGE and result.window == WINDOW
    assert result.changed
    for rel in expected:
        assert _absent_on_purpose(lake_root, rel)
    kept = [_rel(t, d) for d in SESSIONS if d > EDGE for t in TICKERS]
    assert all((lake_root / rel).exists() for rel in kept)
    # One trim line per partition, carrying the manifest's sha and the bucket's version.
    lines = _lines(lake_root)
    assert [line["partition"] for line in lines] == expected
    entries = latest_entries(lake_root)
    for line in lines:
        key = TARGET.key(line["partition"])
        assert line["kind"] == trimmed.TRIM_KIND
        assert line["sha256"] == entries[line["partition"]]["sha256"]
        assert line["version_id"] == client.versions(key)[-1].version_id
        assert line["verified_at"] == line["trimmed_at"] == _et(TONIGHT, 16, 30).isoformat()
    # The ledger's own manifest entry was refreshed with the trim's source.
    assert entries["trimmed.jsonl"]["sha256"] == sha256_file(trimmed.trimmed_path(lake_root))
    assert entries["trimmed.jsonl"]["source"] == trim_module.TRIM_SOURCE
    # Nothing was written to the bucket.
    assert client.puts() == []


def test_the_bucket_versions_differ_per_partition_and_the_line_names_its_own(lake_root):
    # A second version of one key makes its id differ from its neighbours', so a line that
    # recorded the wrong response's id would fail here.
    client = _build(lake_root)
    rel = _rel("SPY", SESSIONS[0])
    client.store(TARGET.key(rel), (lake_root / rel).read_bytes())
    _trim(lake_root, client)
    line = trimmed.latest_trimmed(lake_root)[rel]
    assert line["version_id"] == client.versions(TARGET.key(rel))[-1].version_id
    assert len(client.versions(TARGET.key(rel))) == 2


def test_a_first_night_backlog_is_trimmed_to_the_window_inside_the_deadline(lake_root):
    # Verification 12: more than W sessions on the first night a trim is allowed.
    client = _build(lake_root)
    clock = ManualClock(_et(TONIGHT, 16, 30))
    client.on_get = lambda kwargs: clock.advance(60)
    result = _trim(lake_root, client, clock=clock)
    assert result.deadline is None
    remaining = sorted(
        {p.name for p in (lake_root / "chains").rglob("*.parquet")},
    )
    assert remaining == [f"date={day.isoformat()}.parquet" for day in SESSIONS if day > EDGE]


# -- clause 1: present on disk ------------------------------------------------------------


def test_an_absent_partition_sends_no_read_and_writes_nothing(lake_root):
    client = _build(lake_root)
    lost = _rel("SPY", SESSIONS[0])
    (lake_root / lost).unlink()
    result = _trim(lake_root, client)
    assert lost not in _gets(client)
    assert lost not in [line["partition"] for line in _lines(lake_root)]
    assert lost not in result.trimmed


def test_a_second_run_the_same_evening_sends_no_read_and_appends_nothing(lake_root):
    """Verification 6. Mutation this catches: selecting without the presence check."""
    client = _build(lake_root)
    _trim(lake_root, client)
    before = trimmed.trimmed_path(lake_root).read_bytes()
    manifest = (lake_root / "manifest.jsonl").read_bytes()
    client.calls.clear()

    again = _trim(lake_root, client)

    assert _gets(client) == []
    assert trimmed.trimmed_path(lake_root).read_bytes() == before
    assert (lake_root / "manifest.jsonl").read_bytes() == manifest
    assert not again.changed
    assert again.trimmed == () and again.skipped == ()


# -- clause 2: the window edge ------------------------------------------------------------


def test_a_partition_inside_the_window_is_kept_even_when_its_cutoff_allows_it(lake_root):
    # The cutoffs reach Thursday, past the edge. Only clause 2 keeps Wednesday and Thursday.
    client = _build(lake_root, cutoffs=dict.fromkeys(TICKERS, YESTERDAY))
    result = _trim(lake_root, client)
    assert list(result.trimmed) == _expected()
    assert (lake_root / _rel("SPY", date(2026, 8, 26))).exists()


def test_a_grown_window_trims_to_its_own_edge_not_the_checkpoints(lake_root):
    # The checkpoint was cut at the edge of a window of 3. Grown to 4, the edge moves back to
    # Monday, and the checkpoint's stale cutoff at Tuesday would trim one session too far.
    client = _build(lake_root)
    result = _trim(lake_root, client, window=4)
    assert result.edge == date(2026, 8, 24)
    assert list(result.trimmed) == _expected(edge=date(2026, 8, 24))
    assert (lake_root / _rel("SPY", EDGE)).exists()


# -- clause 3: the watermark -------------------------------------------------------------


def test_a_partition_at_or_past_the_watermark_is_kept_and_not_read(lake_root):
    client = _build(lake_root)
    order = [entry["partition"] for entry in read_manifest(lake_root)]
    held = _rel("QQQ", SESSIONS[2])
    position = order.index(held)
    # The bucket's copy carried every entry before this one, and this one not yet.
    result = _trim(lake_root, client, upload=_upload(lake_root, watermark=position))
    assert held not in result.trimmed
    assert held not in _gets(client)
    assert (lake_root / held).exists()
    assert list(result.trimmed) == [rel for rel in _expected() if order.index(rel) < position]


# -- clause 4: segments ----------------------------------------------------------------


def test_a_ticker_day_with_journal_segments_is_kept(lake_root):
    client = _build(lake_root)
    day = SESSIONS[1]
    segment_dir = LakePaths(lake_root).segment_dir("chains", "SPY", day)
    segment_dir.mkdir(parents=True)
    (segment_dir / "seg-a-1.arrows").write_bytes(b"segment")
    # A file that is not a segment says nothing about the day.
    other = LakePaths(lake_root).segment_dir("chains", "QQQ", day)
    other.mkdir(parents=True)
    (other / "notes.txt").write_text("not a segment")
    result = _trim(lake_root, client)
    held = _rel("SPY", day)
    assert held not in result.trimmed
    assert (lake_root / held).exists()
    assert _rel("QQQ", day) in result.trimmed
    # A partition never trimmed has no trim line to supersede, so it gets no line at all.
    assert held not in result.restored
    assert held not in [line["partition"] for line in _lines(lake_root)]


def test_a_journal_directory_that_cannot_be_listed_counts_as_holding_segments(lake_root):
    client = _build(lake_root)
    day = SESSIONS[1]
    segment_dir = LakePaths(lake_root).segment_dir("chains", "SPY", day)
    segment_dir.mkdir(parents=True)
    segment_dir.chmod(0o000)
    try:
        result = _trim(lake_root, client)
    finally:
        segment_dir.chmod(0o755)
    held = _rel("SPY", day)
    assert held not in result.trimmed
    assert (lake_root / held).exists()
    assert held not in result.restored
    assert held not in [line["partition"] for line in _lines(lake_root)]


def test_the_segment_glob_is_the_one_compaction_reads():
    from lake.paths import SEGMENT_GLOB

    assert SEGMENT_GLOB == "seg-*.arrows"


# -- clause 5 and rot ----------------------------------------------------------------


def test_a_rotted_bucket_copy_is_kept_and_named_with_its_good_lake_copy(lake_root):
    """Verification 4. Mutations this catches: trimming on a mismatch, and any PUT."""
    client = _build(lake_root)
    rel = _rel("SPY", SESSIONS[0])
    client.store(TARGET.key(rel), b"rotted bytes")
    result = _trim(lake_root, client)
    assert rel not in result.trimmed
    assert (lake_root / rel).exists()
    assert rel not in trimmed.latest_trimmed(lake_root)
    (finding,) = result.rot
    assert finding.partition == rel
    assert finding.local == LOCAL_GOOD
    assert finding.version_id == client.versions(TARGET.key(rel))[-1].version_id
    assert finding.manifest_sha256 == latest_entries(lake_root)[rel]["sha256"]
    assert client.puts() == []
    # The rest of the run went on.
    assert len(result.trimmed) == len(_expected()) - 1


def test_two_rotted_copies_fold_into_one_page_body(lake_root):
    client = _build(lake_root)
    first, second = _rel("QQQ", SESSIONS[0]), _rel("SPY", SESSIONS[1])
    client.store(TARGET.key(first), b"rot one")
    (lake_root / second).write_bytes(b"rot two in the lake")
    client.store(TARGET.key(second), b"rot two in the bucket")
    result = _trim(lake_root, client)
    assert [finding.partition for finding in result.rot] == [first, second]
    assert [finding.local for finding in result.rot] == [LOCAL_GOOD, LOCAL_ROTTED]
    body = rot_page_body(result.rot)
    assert first in body and second in body
    assert body.startswith("2 chains partition(s)")
    for finding in result.rot:
        assert f"version {finding.version_id}" in body


def test_the_rot_page_caps_its_list():
    findings = [
        trim_module.RotFinding(f"chains/ticker=T{n}/date=2026-08-17.parquet", "a", "b", "v", "x")
        for n in range(6)
    ]
    body = rot_page_body(findings)
    assert "and 2 more" in body
    assert "T4" not in body
    assert len(body.encode()) < 1000


def test_a_local_copy_that_rotted_beside_a_good_bucket_copy_is_trimmed(lake_root):
    client = _build(lake_root)
    rel = _rel("SPY", SESSIONS[0])
    (lake_root / rel).write_bytes(b"rotted on the volume")
    result = _trim(lake_root, client)
    assert rel in result.trimmed
    assert result.rot == ()
    assert _absent_on_purpose(lake_root, rel)


def test_a_normal_night_never_hashes_a_partition_in_the_lake(lake_root, monkeypatch):
    """Mutation this catches: hashing the local file on every partition."""
    client = _build(lake_root)
    hashed: list[Path] = []
    real = trim_module.sha256_file

    def recording(path):
        hashed.append(Path(path))
        return real(path)

    monkeypatch.setattr(trim_module, "sha256_file", recording)
    _trim(lake_root, client)
    assert all("chains" not in path.parts for path in hashed)


def test_a_key_missing_from_the_bucket_skips_only_its_partition(lake_root):
    client = _build(lake_root)
    rel = _rel("SPY", SESSIONS[0])
    del client.objects[TARGET.key(rel)]
    result = _trim(lake_root, client)
    assert rel not in result.trimmed
    assert (lake_root / rel).exists()
    assert any(line.startswith(f"{rel}: the bucket holds no such key") for line in result.skipped)
    assert len(result.trimmed) == len(_expected()) - 1
    assert result.stopped is None


def test_an_other_bucket_answer_skips_only_its_partition(lake_root):
    client = _build(lake_root)
    rel = _rel("SPY", SESSIONS[0])

    def fail(kwargs):
        if kwargs["Key"] == TARGET.key(rel):
            raise client_error("InvalidObjectState", "GetObject", 400)

    client.on_get = fail
    result = _trim(lake_root, client)
    assert rel not in result.trimmed
    assert f"{rel}: the bucket answered the read with InvalidObjectState" in result.skipped
    assert len(result.trimmed) == len(_expected()) - 1


# -- the run-stopping failures ---------------------------------------------------------


def _first(client: FakeS3) -> int:
    return len(_gets(client))


@pytest.mark.parametrize(
    "error",
    [client_error("AccessDenied", "GetObject", 403), unreachable()],
    ids=["refused", "unreachable"],
)
def test_a_bucket_that_refuses_or_cannot_be_reached_stops_the_run(lake_root, error):
    client = _build(lake_root)
    client.on_get = lambda kwargs: (_ for _ in ()).throw(error)
    result = _trim(lake_root, client)
    assert _first(client) == 1
    assert result.trimmed == ()
    assert result.stopped is not None and "stopped" in result.stopped
    assert _lines(lake_root) == []


@pytest.mark.parametrize("version", [None, "null"])
def test_a_read_that_names_no_version_stops_the_run(lake_root, version):
    class _Unversioned(FakeS3):
        def get_object(self, **kwargs):
            response = super().get_object(**kwargs)
            if version is None:
                del response["VersionId"]
            else:
                response["VersionId"] = version
            return response

    built = _build(lake_root)
    client = _Unversioned()
    client.objects = built.objects
    result = _trim(lake_root, client)
    assert _first(client) == 1
    assert result.trimmed == ()
    assert "Turn versioning on" in str(result.stopped)
    assert _lines(lake_root) == []


@pytest.mark.parametrize(
    "error",
    [
        OSError(errno.ENOSPC, "No space left on device"),
        trimmed.TrimmedLineLost("the line did not read back"),
        RowCountRegression("trimmed.jsonl", 3, 2),
    ],
    ids=["enospc", "line-lost", "row-count"],
)
def test_a_failure_in_the_ledger_step_stops_the_run_and_keeps_the_file(
    lake_root, monkeypatch, error
):
    """Mutations this catches: unlinking before the line, and skipping on a ledger failure."""
    client = _build(lake_root)

    def refuse(*args, **kwargs):
        raise error

    monkeypatch.setattr(trimmed, "append_trimmed", refuse)
    result = _trim(lake_root, client)
    assert _first(client) == 1
    assert result.trimmed == () and result.lined == ()
    assert result.stopped is not None and "trimmed ledger refused a line" in result.stopped
    assert all((lake_root / rel).exists() for rel in _expected())


def test_a_failed_fsync_after_the_line_stops_the_run_before_the_unlink(lake_root, monkeypatch):
    from lake import compact

    client = _build(lake_root)

    def refuse(path):
        raise OSError(errno.EIO, "Input/output error")

    monkeypatch.setattr(compact, "_durable_dir", refuse)
    result = _trim(lake_root, client)
    first = _expected()[0]
    assert (lake_root / first).exists()
    assert result.stopped is not None
    # The line landed and the file stayed, which reads as present and is retried.
    assert trimmed.latest_trimmed(lake_root)[first]["kind"] == trimmed.TRIM_KIND
    assert not _absent_on_purpose(lake_root, first)


def _failing_unlink(monkeypatch, target: str, code: int):
    real = Path.unlink

    def unlink(self, missing_ok=False):
        if self.as_posix().endswith(target):
            raise OSError(code, os.strerror(code), str(self))
        return real(self, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", unlink)


def test_an_unlink_eio_stops_the_run(lake_root, monkeypatch):
    client = _build(lake_root)
    first = _expected()[0]
    _failing_unlink(monkeypatch, first, errno.EIO)
    result = _trim(lake_root, client)
    assert _first(client) == 1
    assert result.lined == (first,)
    assert result.trimmed == ()
    assert result.stopped is not None and "unlink" in result.stopped
    assert (lake_root / first).exists()


@pytest.mark.parametrize("code", [errno.EACCES, errno.EPERM])
def test_an_unlink_refused_by_permission_stops_only_its_ticker(lake_root, monkeypatch, code):
    """Mutations this catches: stopping the run rather than the ticker on EACCES, and stopping
    on every OSError."""
    client = _build(lake_root)
    first = _rel("QQQ", SESSIONS[0])
    _failing_unlink(monkeypatch, first, code)
    result = _trim(lake_root, client)
    assert result.stopped is None
    assert result.lined == (first,)
    # QQQ's later candidates were not read, and SPY trimmed in full.
    assert [rel for rel in _gets(client) if "QQQ" in rel] == [first]
    assert list(result.trimmed) == [rel for rel in _expected() if "SPY" in rel]
    (held,) = result.held
    assert held.startswith("chains/ticker=QQQ/: ")
    assert "chown" in held


def test_a_ticker_directory_that_cannot_be_searched_holds_only_its_ticker(lake_root):
    """Mutations this catches: letting the ``lstat`` error stop the run, and naming the
    directory by its absolute path."""
    client = _build(lake_root)
    ticker_dir = lake_root / "chains" / "ticker=QQQ"
    ticker_dir.chmod(0o600)
    try:
        result = _trim(lake_root, client)
    finally:
        ticker_dir.chmod(0o755)
    assert result.stopped is None and result.refused is None
    assert [rel for rel in _gets(client) if "QQQ" in rel] == []
    assert list(result.trimmed) == [rel for rel in _expected() if "SPY" in rel]
    assert all((lake_root / rel).exists() for rel in _expected() if "QQQ" in rel)
    (held,) = result.held
    assert held.startswith("chains/ticker=QQQ/: ")
    assert "chown" in held and "chmod" in held
    assert str(lake_root) not in "\n".join(result.render())


def test_a_stopped_line_names_its_path_relative_to_the_lake_root(lake_root, monkeypatch):
    """Mutation this catches: printing the ``OSError`` with its absolute filename."""
    client = _build(lake_root)
    first = _expected()[0]
    _failing_unlink(monkeypatch, first, errno.EIO)
    result = _trim(lake_root, client)
    assert result.stopped is not None
    assert str(lake_root) not in result.stopped
    assert str(lake_root.resolve()) not in result.stopped
    assert first in result.stopped


def test_an_unlink_of_a_file_already_gone_counts_as_done(lake_root, monkeypatch):
    client = _build(lake_root)
    first = _expected()[0]
    real = Path.unlink

    def vanishes(self, missing_ok=False):
        real(self, missing_ok=missing_ok)
        if self.as_posix().endswith(first):
            raise FileNotFoundError(errno.ENOENT, "gone", str(self))

    monkeypatch.setattr(Path, "unlink", vanishes)
    result = _trim(lake_root, client)
    assert first in result.trimmed
    assert result.lined == ()
    assert result.stopped is None


def test_an_unforeseen_error_becomes_a_stopped_result(lake_root, monkeypatch):
    client = _build(lake_root)
    monkeypatch.setattr(trim_module, "segments_remain", lambda *a: 1 / 0)
    result = _trim(lake_root, client)
    assert result.stopped is not None and "ZeroDivisionError" in result.stopped


# -- the deadline ---------------------------------------------------------------------


def test_the_trim_stops_at_the_uploads_deadline_between_partitions(lake_root):
    client = _build(lake_root)
    clock = ManualClock(_et(TONIGHT, 16, 30))
    deadline = _et(TONIGHT, 17, 0)
    # Each read takes ten minutes, so three partitions fit before the deadline.
    client.on_get = lambda kwargs: clock.advance(600)
    result = _trim(lake_root, client, clock=clock, upload=_upload(lake_root, deadline=deadline))
    assert list(result.trimmed) == _expected()[:3]
    assert result.deadline is not None and deadline.isoformat() in result.deadline
    assert (lake_root / _expected()[3]).exists()


# -- clause 6: the checkpoint and tonight -----------------------------------------------


def test_a_partition_past_its_tickers_cutoff_is_kept(lake_root):
    client = _build(lake_root, cutoffs={"QQQ": EDGE, "SPY": SESSIONS[1]})
    result = _trim(lake_root, client)
    assert list(result.trimmed) == [
        rel for rel in _expected() if "QQQ" in rel or rel <= _rel("SPY", SESSIONS[1])
    ]
    assert (lake_root / _rel("SPY", SESSIONS[2])).exists()


def test_a_ticker_with_no_checkpoint_entry_keeps_every_partition(lake_root):
    client = _build(lake_root, cutoffs={"QQQ": EDGE})
    result = _trim(lake_root, client)
    assert all("SPY" not in rel for rel in result.trimmed)
    assert list(result.trimmed) == [rel for rel in _expected() if "QQQ" in rel]


def test_a_date_min_cutoff_keeps_every_partition_of_its_ticker(lake_root):
    client = _build(lake_root, cutoffs={"QQQ": EDGE, "SPY": date.min})
    result = _trim(lake_root, client)
    assert all("SPY" not in rel for rel in result.trimmed)
    assert result.stopped is None


@pytest.mark.parametrize(
    ("moment", "calendar"),
    [
        (_et(TONIGHT, 16, 30), weekday_sessions(FIRST_MONDAY, SECOND_MONDAY, holidays={TONIGHT})),
        (_et(date(2026, 8, 29), 16, 30), CALENDAR),
    ],
    ids=["holiday", "weekend"],
)
def test_a_night_that_is_not_a_session_refuses(lake_root, moment, calendar):
    client = _build(lake_root)
    result = _trim(lake_root, client, clock=ManualClock(moment), calendar=calendar)
    assert result.refused is not None and "is not a session" in result.refused
    assert _gets(client) == []
    assert not trimmed.trimmed_path(lake_root).exists()


@pytest.mark.parametrize(
    "moment",
    [_et(TONIGHT, 9, 0), _et(TONIGHT, 16, 20, 0), _et(TONIGHT, 16, 20, 30)],
    ids=["the-morning-after", "the-deadline-minute", "inside-the-deadline-minute"],
)
def test_a_hand_run_before_tonights_option_close_deadline_refuses(lake_root, moment):
    """Mutations this catches: dropping the option-close gate, and comparing ``clock.now()``
    rather than the minute slot. At 16:20:30 the clock is past 16:20 and the slot is not."""
    client = _build(lake_root)
    result = _trim(lake_root, client, clock=ManualClock(moment))
    assert result.refused is not None and "has not passed" in result.refused
    assert _gets(client) == []


def test_a_run_the_minute_after_the_deadline_trims(lake_root):
    client = _build(lake_root)
    result = _trim(lake_root, client, clock=ManualClock(_et(TONIGHT, 16, 21)))
    assert result.refused is None
    assert list(result.trimmed) == _expected()


def test_a_run_after_the_sweep_on_the_checkpoints_own_session_refuses(lake_root):
    """Mutation this catches: dropping the strictly-after rule."""
    client = _build(lake_root, session_day=TONIGHT)
    result = _trim(lake_root, client, clock=ManualClock(_et(TONIGHT, 19, 0)))
    assert result.refused is not None and "the checkpoint is tonight's" in result.refused
    assert _gets(client) == []


def test_no_checkpoint_refuses(lake_root):
    client = _build(lake_root, session_day=None)
    result = _trim(lake_root, client)
    assert result.refused is not None and "no split checkpoint" in result.refused


def test_a_checkpoint_whose_sha_disagrees_with_its_entry_refuses(lake_root):
    import pyarrow.parquet as pq

    client = _build(lake_root)
    # A readable checkpoint whose cutoffs are not the ones the sweep recorded.
    wider = Checkpoint(
        session_day=YESTERDAY,
        entries=tuple(CheckpointEntry(_state(t, YESTERDAY), ()) for t in TICKERS),
    )
    pq.write_table(_to_table(wider), checkpoint_path(lake_root))
    result = _trim(lake_root, client)
    assert result.refused is not None and "does not hash to its manifest entry" in result.refused
    assert _gets(client) == []


def test_a_checkpoint_with_no_manifest_entry_refuses(lake_root):
    import pyarrow.parquet as pq

    client = _build(lake_root, session_day=None)
    checkpoint = Checkpoint(
        session_day=YESTERDAY,
        entries=tuple(CheckpointEntry(_state(t, EDGE), ()) for t in TICKERS),
    )
    path = checkpoint_path(lake_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(_to_table(checkpoint), path)
    result = _trim(lake_root, client)
    assert result.refused is not None and "has no manifest entry" in result.refused
    assert _gets(client) == []


def test_an_unreadable_checkpoint_refuses(lake_root):
    client = _build(lake_root)
    checkpoint_path(lake_root).write_bytes(b"not parquet")
    result = _trim(lake_root, client)
    assert result.refused is not None and "18:30 sweep rewrites it" in result.refused


def test_an_edge_the_calendar_cannot_count_refuses(lake_root):
    client = _build(lake_root)
    result = _trim(lake_root, client, window=400)
    assert result.refused is not None and "window edge could not be counted" in result.refused


def _restore(root: Path, rel: str, restored_at: object) -> None:
    line = {
        trimmed.KIND_FIELD: trimmed.RESTORE_KIND,
        trimmed.PARTITION_FIELD: rel,
        trimmed.SHA256_FIELD: latest_entries(root)[rel]["sha256"],
    }
    if restored_at is not ...:
        line[trimmed.RESTORED_AT_FIELD] = restored_at
    with lake_lock(root):
        trimmed.append_trimmed(root, line, source="range-restore", fetched_at=None)


@pytest.mark.parametrize(
    ("restored_at", "kept"),
    [
        ("2026-08-26T19:00:00-04:00", False),  # before the checkpoint's session day
        ("2026-08-27T09:00:00-04:00", True),  # on the checkpoint's own session day
        ("2026-08-27T02:00:00+00:00", False),  # 22:00 Eastern on the 26th
        ("2026-08-26T19:00:00", True),  # no offset
        ("yesterday", True),  # unparseable
        (20260826, True),  # not text
        (..., True),  # missing
    ],
    ids=["before", "on-the-day", "utc-before", "no-offset", "unparseable", "not-text", "missing"],
)
def test_a_restore_waits_for_a_checkpoint_written_after_it(lake_root, restored_at, kept):
    """Mutations this catches: ``<`` turned to ``<=``, and dropping the restore-line rule."""
    client = _build(lake_root)
    rel = _rel("SPY", SESSIONS[0])
    _restore(lake_root, rel, restored_at)
    result = _trim(lake_root, client)
    assert (rel in result.trimmed) is not kept
    assert (lake_root / rel).exists() is kept


def test_a_line_of_an_unknown_kind_keeps_its_partition(lake_root):
    client = _build(lake_root)
    rel = _rel("SPY", SESSIONS[0])
    with lake_lock(lake_root):
        trimmed.append_trimmed(
            lake_root, {"kind": "hold", "partition": rel}, source="test", fetched_at=None
        )
    result = _trim(lake_root, client)
    assert rel not in result.trimmed


# -- clause 7: the quarantine -----------------------------------------------------------


def test_a_withheld_partition_is_kept(lake_root):
    held = _rel("SPY", SESSIONS[0])
    client = _build(lake_root, quarantine=(held,))
    result = _trim(lake_root, client)
    assert held not in result.trimmed
    assert held not in _gets(client)
    assert (lake_root / held).exists()
    assert len(result.trimmed) == len(_expected()) - 1


# -- order and recovery ----------------------------------------------------------------


def _crashed_before_unlink(root: Path, rel: str, version: str = "v-old") -> None:
    """A trim line beside a present file, which a crash between the line and the unlink leaves."""
    line = trimmed.trim_line(
        rel,
        sha256=latest_entries(root)[rel]["sha256"],
        version_id=version,
        verified_at="2026-08-27T16:31:00-04:00",
        trimmed_at="2026-08-27T16:31:00-04:00",
    )
    with lake_lock(root):
        trimmed.append_trimmed(root, line, source="trim", fetched_at=None)


def test_every_crash_point_reads_as_present_or_as_a_designed_absence(lake_root):
    client = _build(lake_root)
    rel = _rel("SPY", SESSIONS[0])
    entries = latest_entries(lake_root)
    # Before the line: present, no line.
    assert (lake_root / rel).exists()
    # After the line, before the unlink: the line is latest and the file is present, which
    # every reader treats as present.
    _crashed_before_unlink(lake_root, rel)
    assert (lake_root / rel).exists()
    # After the unlink: a designed absence.
    (lake_root / rel).unlink()
    assert trimmed.is_designed_absence(rel, entries, trimmed.latest_trimmed(lake_root))
    # The next run sends no read for it and writes nothing for it.
    result = _trim(lake_root, client)
    assert rel not in _gets(client)
    assert [line["partition"] for line in _lines(lake_root)].count(rel) == 1
    assert rel not in result.trimmed


def test_recovery_of_a_file_that_still_qualifies_writes_a_fresh_line_with_the_new_version(
    lake_root,
):
    """Mutation this catches: reusing the old line's ``version_id``."""
    client = _build(lake_root)
    rel = _rel("SPY", SESSIONS[0])
    _crashed_before_unlink(lake_root, rel)
    result = _trim(lake_root, client)
    assert rel in result.trimmed
    lines = [line for line in _lines(lake_root) if line["partition"] == rel]
    assert len(lines) == 2
    assert lines[-1]["version_id"] == client.versions(TARGET.key(rel))[-1].version_id
    assert lines[-1]["version_id"] != "v-old"
    assert _absent_on_purpose(lake_root, rel)


def test_recovery_after_the_window_grew_writes_a_restore_line_and_keeps_the_file(lake_root):
    """Mutation this catches: re-checking only the sha on recovery."""
    client = _build(lake_root)
    rel = _rel("SPY", EDGE)
    _crashed_before_unlink(lake_root, rel)
    result = _trim(lake_root, client, window=4)
    assert rel in result.restored and rel not in result.trimmed
    assert (lake_root / rel).exists()
    latest = trimmed.latest_trimmed(lake_root)[rel]
    assert latest["kind"] == trimmed.RESTORE_KIND
    assert latest["sha256"] == latest_entries(lake_root)[rel]["sha256"]
    assert latest["restored_at"] == _et(TONIGHT, 16, 30).isoformat()
    assert rel not in _gets(client)
    assert result.changed


@pytest.mark.parametrize("clause", ["cutoff", "quarantine", "segments", "rot", "missing-key"])
def test_recovery_meeting_a_definite_no_writes_a_restore_line(lake_root, clause):
    rel = _rel("SPY", SESSIONS[0])
    cutoffs = {"QQQ": EDGE, "SPY": date.min} if clause == "cutoff" else None
    quarantine = (rel,) if clause == "quarantine" else ()
    client = _build(lake_root, cutoffs=cutoffs, quarantine=quarantine)
    if clause == "segments":
        segment_dir = LakePaths(lake_root).segment_dir("chains", "SPY", SESSIONS[0])
        segment_dir.mkdir(parents=True)
        (segment_dir / "seg-a-1.arrows").write_bytes(b"segment")
    if clause == "rot":
        client.store(TARGET.key(rel), b"rotted")
    if clause == "missing-key":
        del client.objects[TARGET.key(rel)]
    _crashed_before_unlink(lake_root, rel)
    result = _trim(lake_root, client)
    assert rel in result.restored
    assert (lake_root / rel).exists()
    assert trimmed.latest_trimmed(lake_root)[rel]["kind"] == trimmed.RESTORE_KIND


def test_recovery_cut_off_by_the_deadline_or_a_stop_writes_nothing(lake_root):
    client = _build(lake_root)
    rel = _rel("QQQ", SESSIONS[0])
    _crashed_before_unlink(lake_root, rel)
    before = trimmed.trimmed_path(lake_root).read_bytes()
    clock = ManualClock(_et(TONIGHT, 18, 0))
    result = _trim(lake_root, client, clock=clock)
    assert result.deadline is not None
    assert trimmed.trimmed_path(lake_root).read_bytes() == before
    client.fail_with = client_error("AccessDenied", "GetObject", 403)
    stopped = _trim(lake_root, client)
    assert stopped.stopped is not None
    assert trimmed.trimmed_path(lake_root).read_bytes() == before


def test_the_line_and_its_directory_are_fsynced_before_the_unlink(lake_root, monkeypatch):
    """Mutations this catches: dropping either fsync, or moving it after the unlink."""
    from lake import compact

    client = _build(lake_root, days=SESSIONS[:1], tickers=("SPY",))
    order: list[str] = []
    real_unlink = Path.unlink
    monkeypatch.setattr(compact, "_durable", lambda path: order.append(f"file {Path(path).name}"))
    monkeypatch.setattr(compact, "_durable_dir", lambda path: order.append(f"dir {Path(path)}"))

    def unlink(self, missing_ok=False):
        order.append(f"unlink {self.name}")
        return real_unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", unlink)
    result = _trim(lake_root, client)
    assert result.trimmed == (_rel("SPY", SESSIONS[0]),)
    assert order == [
        "file trimmed.jsonl",
        f"dir {lake_root}",
        f"unlink date={SESSIONS[0].isoformat()}.parquet",
    ]


def test_an_upload_without_a_deadline_or_watermark_refuses(lake_root):
    client = _build(lake_root)
    for upload in (
        UploadSummary(target=str(TARGET), deadline=_et(TONIGHT, 17, 45), watermark=None),
        UploadSummary(target=str(TARGET), deadline=None, watermark=100),
    ):
        result = _trim(lake_root, client, upload=upload)
        assert result.refused is not None and "no deadline or watermark" in result.refused
    assert _gets(client) == []


def test_the_ledger_gains_one_line_per_trim_and_the_file_goes_after_it(lake_root, monkeypatch):
    """Mutation this catches: unlinking before the line lands."""
    client = _build(lake_root)
    seen: list[tuple[str, bool]] = []
    real = trimmed.append_trimmed

    def recording(root, line, **kwargs):
        rel = line["partition"]
        seen.append((rel, (Path(root) / rel).exists()))
        return real(root, line, **kwargs)

    monkeypatch.setattr(trimmed, "append_trimmed", recording)
    _trim(lake_root, client)
    assert seen and all(present for _, present in seen)


def test_the_result_renders_each_outcome(lake_root):
    result = TrimResult(
        window=3,
        edge=EDGE,
        trimmed=("a",),
        lined=("b",),
        restored=("c",),
        skipped=("d: why",),
        held=("chains/ticker=X/: why",),
        deadline="reached",
        stopped="because",
    )
    lines = result.render()
    assert lines[0] == "  trim     window=3 edge=2026-08-25 trimmed=1 restored=1 skipped=1 rot=0"
    assert "  trim     line written, file kept: b" in lines
    assert "  trim     restore line: c" in lines
    assert "  trim     skipped d: why" in lines
    assert "  trim     held chains/ticker=X/: why" in lines
    assert "  trim     deadline: reached" in lines
    assert "  trim     stopped: because" in lines
    assert TrimResult(refused="no").render() == ["  trim     refused: no"]
    assert not TrimResult(skipped=("x",), refused=None).changed
    assert TrimResult(lined=("x",)).changed
    assert TrimResult(restored=("x",)).changed
    assert TrimResult(trimmed=("x",)).changed


def test_nothing_reads_json_from_the_ledger_but_the_ledger_module(lake_root):
    # The trim writes only through ``append_trimmed``, so every line reads back as JSON.
    client = _build(lake_root)
    _trim(lake_root, client)
    for raw in trimmed.trimmed_path(lake_root).read_text().splitlines():
        assert isinstance(json.loads(raw), dict)
