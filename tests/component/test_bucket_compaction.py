"""The close+15 compaction with a bucket target.

``BucketBackup`` runs where ``RsyncBackup`` runs, inside compaction's lake-root lock,
chosen by the form of ``backup_target``. The ping still attests the backup: it fires only
after the upload, and an upload that refuses or reaches its deadline leaves it unsent so
healthchecks pages. ``compact.main`` reports a bucket refusal as one line and exit 2.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from lake import bucket
from lake import compact as compact_module
from lake.bucket import BucketBackup, UploadDeadline, WatermarkMissing, first_upload
from lake.chain_plan import ChainPlanError
from lake.compact import COMPACTION_SLUG, compact
from lake.config import BucketTarget
from lake.paths import LakePaths
from tests.component.test_compaction import (
    DAY,
    FRIDAY,
    TUESDAY,
    URL,
    _calendar,
    _chains,
    _clock_at,
    _segment,
    _snap,
)
from tests.support.bucket import FakeS3
from tests.support.config import write_config
from tests.support.lake import FixtureLake
from tests.support.pinger import FakePinger

TARGET = BucketTarget(bucket="lake-backup", prefix="lake")
KEYS = (
    "bucket_access_key_id: AKIDCONFIG\n"
    "bucket_secret_access_key: secret-bucket-key\n"
    "bucket_region: us-east-2\n"
)


def _seeded(lake_root: Path) -> FakeS3:
    """A bucket the first upload seeded the evening before, from a lake holding one day.

    The seeded lake is never empty. A bucket seeded from an empty manifest carries a
    zero-byte copy, which the nightly upload refuses as no watermark.
    """
    FixtureLake(lake_root).with_partition(
        "chains", "SPY", FRIDAY, _chains(1, snap_ts=_snap(FRIDAY, 0))
    ).build()
    client = FakeS3()
    first_upload(
        lake_root, TARGET, client=client, clock=_clock_at(FRIDAY, 19, 0), calendar=_calendar()
    )
    client.calls.clear()
    return client


def _job(lake_root: Path, client: FakeS3, events: list[str], *, clock=None):
    clock = clock if clock is not None else _clock_at(DAY, 16, 30)
    client.on_put = lambda kwargs, data: events.append(f"put {kwargs['Key']}")
    return compact(
        lake_root,
        clock=clock,
        calendar=_calendar(),
        backup=BucketBackup(client=client, clock=clock, calendar=_calendar()),
        backup_target=TARGET,
        pinger=FakePinger(events),
        ping_url=URL,
        plan_path=lake_root.parent / "chain_plan.json",
    )


def test_the_sealed_day_goes_up_and_the_ping_comes_after_the_manifest(lake_root):
    client = _seeded(lake_root)
    _segment(lake_root, "chains", "SPY", DAY, _chains(2, snap_ts=_snap(DAY, 0)), start_ts="a")
    events: list[str] = []

    result = _job(lake_root, client, events)

    partition = LakePaths(lake_root).chains_partition_path("SPY", DAY)
    rel = partition.relative_to(lake_root).as_posix()
    assert result.backed_up and result.pinged
    assert events == [f"put lake/{rel}", "put lake/manifest.jsonl", "ping"]


def test_an_empty_bucket_raises_before_the_ping(lake_root):
    _segment(lake_root, "chains", "SPY", DAY, _chains(2, snap_ts=_snap(DAY, 0)), start_ts="a")
    events: list[str] = []
    with pytest.raises(WatermarkMissing):
        _job(lake_root, FakeS3(), events)
    assert "ping" not in events
    # The seal itself stood. The day is single-copy, which the missed ping reports.
    assert LakePaths(lake_root).chains_partition_path("SPY", DAY).exists()


def test_a_deadline_raises_before_the_ping(lake_root):
    client = _seeded(lake_root)
    _segment(lake_root, "chains", "SPY", DAY, _chains(2, snap_ts=_snap(DAY, 0)), start_ts="a")
    events: list[str] = []
    clock = _clock_at(DAY, 16, 30)

    def slow(kwargs, data):
        events.append(f"put {kwargs['Key']}")
        clock.advance(bucket.NIGHTLY_UPLOAD_BUDGET.total_seconds())

    client.on_put = slow
    with pytest.raises(UploadDeadline):
        compact(
            lake_root,
            clock=clock,
            calendar=_calendar(),
            backup=BucketBackup(client=client, clock=clock, calendar=_calendar()),
            backup_target=TARGET,
            pinger=FakePinger(events),
            ping_url=URL,
            plan_path=lake_root.parent / "chain_plan.json",
        )
    assert len(events) == 1 and events[0].startswith("put lake/chains/")


def test_the_backup_hands_its_budget_to_the_upload(lake_root):
    # A budget of nothing puts the deadline at the start, so the upload stops before its
    # first request. The default 75 minutes would let it run.
    client = _seeded(lake_root)
    clock = _clock_at(DAY, 16, 30)
    backup = BucketBackup(client=client, clock=clock, calendar=_calendar(), budget=timedelta(0))
    with pytest.raises(UploadDeadline, match="0 minutes after it started"):
        backup.sync(lake_root, TARGET)
    assert client.calls == []


def _bucket_config(tmp_path: Path, lake_root: Path) -> Path:
    config = write_config(tmp_path, lake_root)
    text = config.read_text().replace(
        f"backup_target: {tmp_path / 'ssd'}", f"backup_target: {TARGET}"
    )
    config.write_text(text + KEYS)
    return config


def test_main_uploads_to_a_bucket_target_and_pings(lake_root, tmp_path, monkeypatch, capsys):
    client = _seeded(lake_root)
    _segment(lake_root, "chains", "SPY", DAY, _chains(2, snap_ts=_snap(DAY, 0)), start_ts="a")
    config = _bucket_config(tmp_path, lake_root)
    pinger = FakePinger()
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    monkeypatch.setattr("lake.runner.UrllibPinger", lambda: pinger)
    monkeypatch.setattr(
        compact_module, "RsyncBackup", lambda: pytest.fail("a bucket target never runs rsync")
    )

    code = compact_module.main(
        ["--config", str(config), "--plan", str(tmp_path / "chain_plan.json")],
        clock=_clock_at(DAY, 16, 30),
        calendar=_calendar(),
    )

    assert code == 0
    assert client.put_keys()[-1] == "lake/manifest.jsonl"
    assert pinger.urls == [f"https://hc-ping.com/secret-key/{COMPACTION_SLUG}"]
    captured = capsys.readouterr()
    # The night's throughput reaches compaction's log as one line. The sealed partition
    # and the manifest went up, and no other file needed comparing.
    lines = [line for line in captured.out.splitlines() if "Mbit/s" in line]
    assert len(lines) == 1
    assert lines[0].startswith("compact: uploaded 2 file(s), ")
    assert lines[0].endswith("skipped 0 already in the bucket: s3://lake-backup/lake")
    for value in ("secret-bucket-key", "AKIDCONFIG"):
        assert value not in captured.out and value not in captured.err


def test_main_refuses_an_empty_bucket_with_one_line_and_no_ping(
    lake_root, tmp_path, monkeypatch, capsys
):
    config = _bucket_config(tmp_path, lake_root)
    pinger = FakePinger()
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: FakeS3())
    monkeypatch.setattr("lake.runner.UrllibPinger", lambda: pinger)

    with pytest.raises(SystemExit) as exited:
        compact_module.main(
            ["--config", str(config), "--plan", str(tmp_path / "chain_plan.json")],
            clock=_clock_at(DAY, 16, 30),
            calendar=_calendar(),
        )

    assert exited.value.code == 2
    err = capsys.readouterr().err.splitlines()
    assert len(err) == 1
    assert err[0].startswith("compact: ")
    assert bucket.FIRST_UPLOAD_COMMAND in err[0]
    assert pinger.urls == []


def test_main_hands_the_bucket_keys_to_the_page_publisher(lake_root, tmp_path, monkeypatch):
    # A page that carried the bucket's secret key would publish it to ntfy. The
    # publisher refuses a page holding any value it is handed, so the key must be one.
    client = _seeded(lake_root)
    config = _bucket_config(tmp_path, lake_root)
    built: dict = {}
    real = compact_module.Publisher

    def watched(**kwargs):
        built.update(kwargs)
        return real(**kwargs)

    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    monkeypatch.setattr("lake.runner.UrllibPinger", lambda: FakePinger())
    monkeypatch.setattr(compact_module, "Publisher", watched)

    compact_module.main(
        ["--config", str(config), "--plan", str(tmp_path / "chain_plan.json")],
        clock=_clock_at(DAY, 16, 30),
        calendar=_calendar(),
    )

    assert "secret-bucket-key" in built["secrets"]
    assert "AKIDCONFIG" in built["secrets"]


# -- which errors main turns into one line ---------------------------------------


def _seal_one(lake_root: Path) -> None:
    _segment(lake_root, "chains", "SPY", DAY, _chains(2, snap_ts=_snap(DAY, 0)), start_ts="a")


def test_the_path_form_still_raises_a_chain_plan_error(lake_root, tmp_path, monkeypatch):
    # The bucket refusal is caught for the bucket form alone. A chain plan error on the
    # path form stays the traceback and exit 1 it always was, rather than one line and
    # exit 2. The job itself is replaced, so the error reaches main from inside it.
    config = write_config(tmp_path, lake_root)

    def bad_plan(*args, **kwargs):
        raise ChainPlanError("'windows' must be a list")

    monkeypatch.setattr(compact_module, "compact", bad_plan)
    monkeypatch.setattr("lake.runner.UrllibPinger", lambda: FakePinger())

    with pytest.raises(ChainPlanError):
        compact_module.main(
            ["--config", str(config), "--plan", str(tmp_path / "chain_plan.json")],
            clock=_clock_at(DAY, 16, 30),
            calendar=_calendar(),
        )


@pytest.mark.parametrize(
    ("extra", "target", "named"),
    [
        ("", str(TARGET), "bucket_secret_access_key"),
        (KEYS, "s3://Legacy_Bucket/lake", "names no valid bucket"),
        (KEYS.replace("us-east-2", "us east 2"), str(TARGET), "bucket_region"),
    ],
)
def test_bad_bucket_settings_fail_the_backup_after_the_seal(
    lake_root, tmp_path, monkeypatch, capsys, extra, target, named
):
    # The settings are checked when the backup runs, so the day is sealed first, and the
    # missing ping is what pages. No client is ever built.
    _seal_one(lake_root)
    config = write_config(tmp_path, lake_root)
    config.write_text(
        config.read_text().replace(f"backup_target: {tmp_path / 'ssd'}", f"backup_target: {target}")
        + extra
    )
    pinger = FakePinger()
    monkeypatch.setattr("lake.runner.UrllibPinger", lambda: pinger)
    monkeypatch.setattr(bucket, "_build_client", lambda cfg: pytest.fail("no client is built"))

    with pytest.raises(SystemExit) as exited:
        compact_module.main(
            ["--config", str(config), "--plan", str(tmp_path / "chain_plan.json")],
            clock=_clock_at(DAY, 16, 30),
            calendar=_calendar(),
        )

    assert exited.value.code == 2
    err = capsys.readouterr().err.splitlines()
    assert len(err) == 1 and err[0].startswith("compact: ")
    assert named in err[0]
    assert LakePaths(lake_root).chains_partition_path("SPY", DAY).exists()
    assert pinger.urls == []


def test_a_hand_run_during_a_session_seals_and_sends_the_bucket_nothing(lake_root):
    # The catch-up run compact.main describes, started at 11:00 the next session day.
    # The seal still runs, since it is local and short. The upload finds the session
    # open and raises before any request, so the lock goes back to capture and the
    # missing ping pages.
    client = _seeded(lake_root)
    _seal_one(lake_root)
    events: list[str] = []

    with pytest.raises(UploadDeadline, match="next session's capture start"):
        _job(lake_root, client, events, clock=_clock_at(TUESDAY, 11, 0))

    assert client.calls == []
    assert "ping" not in events
    assert LakePaths(lake_root).chains_partition_path("SPY", DAY).exists()
