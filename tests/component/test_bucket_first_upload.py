"""The first-upload command, ``python -m lake.bucket first-upload``, driven through ``main``.

The owner runs it by hand to seed the bucket, and again to re-baseline a bucket whose
copy of ``manifest.jsonl`` stopped being a prefix of the lake's. It compares every object
rather than trusting a watermark, prints its throughput, and refuses with one printed
line and exit 2 rather than a traceback. ``main`` builds the client from the config, so a
test replaces ``bucket.client_from_config`` and drives the rest of the wiring unchanged.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest

from lake import bucket
from lake.bucket import UploadSummary, nightly_upload
from lake.calendar import MARKET_TZ
from lake.config import BucketTarget
from lake.manifest import manifest_path
from tests.support.bucket import FakeS3, client_error, unreachable
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.lake import FixtureLake

DAY = date(2026, 8, 28)
TARGET = "s3://lake-backup/lake"
# A Monday evening after the sweep, and a Sunday inside the scrub window.
MONDAY_19 = datetime(2026, 8, 31, 19, 0, tzinfo=MARKET_TZ)
SUNDAY_20 = datetime(2026, 8, 30, 20, 0, tzinfo=MARKET_TZ)
SUNDAY_1950 = datetime(2026, 8, 30, 19, 50, tzinfo=MARKET_TZ)
SUNDAY_2330 = datetime(2026, 8, 30, 23, 30, tzinfo=MARKET_TZ)
CALENDAR = weekday_sessions(date(2026, 8, 24), date(2026, 8, 31))

KEYS = (
    "bucket_access_key_id: AKIDCONFIG\n"
    "bucket_secret_access_key: secret-bucket-key\n"
    "bucket_region: us-east-2\n"
)


def _setup(tmp_path: Path, *, keys: bool = True) -> tuple[Path, Path]:
    lake = FixtureLake(tmp_path / "lake").with_chains("SPY", DAY).with_quotes("SPY", DAY).build()
    (lake / "reports").mkdir()
    (lake / "reports" / "date=2026-08-28.md").write_text("report\n")
    config = write_config(tmp_path, lake)
    if keys:
        config.write_text(config.read_text() + KEYS)
    return lake, config


def _main(config: Path, client: FakeS3 | None, monkeypatch, *, now=MONDAY_19, target=TARGET):
    if client is not None:
        monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    argv = ["first-upload", "--config", str(config)]
    if target is not None:
        argv += ["--target", target]
    return bucket.main(argv, clock=ManualClock(now), calendar=CALENDAR)


def test_it_uploads_the_whole_lake_with_the_manifest_last(tmp_path, monkeypatch, capsys):
    lake, config = _setup(tmp_path)
    client = FakeS3()

    assert _main(config, client, monkeypatch) == 0

    files = sorted(p.relative_to(lake).as_posix() for p in lake.rglob("*") if p.is_file())
    assert client.keys() == sorted(f"lake/{rel}" for rel in files)
    assert client.put_keys()[-1] == "lake/manifest.jsonl"
    captured = capsys.readouterr()
    out = captured.out
    assert out.startswith("first-upload: uploaded 4 file(s)")
    assert "Mbit/s" in out
    for value in ("secret-bucket-key", "AKIDCONFIG"):
        assert value not in out and value not in captured.err


def test_a_second_run_compares_every_object_and_sends_nothing(tmp_path, monkeypatch, capsys):
    _, config = _setup(tmp_path)
    client = FakeS3()
    _main(config, client, monkeypatch)
    client.calls.clear()

    assert _main(config, client, monkeypatch) == 0

    assert client.puts() == []
    out = capsys.readouterr().out
    assert "uploaded 0 file(s)" in out
    # A copy that was already whole was not replaced, so the run does not say it was.
    assert "replaced" not in out


def test_it_re_baselines_a_copy_that_is_not_a_prefix(tmp_path, monkeypatch, capsys):
    lake, config = _setup(tmp_path)
    client = FakeS3()
    _main(config, client, monkeypatch)
    raw = manifest_path(lake).read_bytes()
    client.store("lake/manifest.jsonl", b"#" + raw[1:])
    capsys.readouterr()

    assert _main(config, client, monkeypatch) == 0

    assert client.body("lake/manifest.jsonl") == raw
    assert "replaced it" in capsys.readouterr().out
    # The nightly upload works again on the next night.
    nightly_upload(
        lake,
        BucketTarget("lake-backup", "lake"),
        client=client,
        clock=ManualClock(MONDAY_19),
        calendar=CALENDAR,
    )


def test_the_backup_target_is_used_when_it_is_a_bucket(tmp_path, monkeypatch):
    lake, config = _setup(tmp_path)
    text = config.read_text().replace(
        f"backup_target: {tmp_path / 'ssd'}", f"backup_target: {TARGET}"
    )
    config.write_text(text)
    client = FakeS3()
    assert _main(config, client, monkeypatch, target=None) == 0
    assert "lake/manifest.jsonl" in client.keys()


def test_the_summary_line_reports_megabits_per_second():
    summary = UploadSummary(target=TARGET, puts=2, put_bytes=10_000_000, seconds=8.0)
    assert "10.0 MB in 8 s (10.0 Mbit/s)" in summary.render()


# -- refusals, each one line and exit 2 --------------------------------------------


def _refused(capsys, code_or_exc) -> str:
    assert code_or_exc.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    lines = captured.err.splitlines()
    assert len(lines) == 1, captured.err
    assert lines[0].startswith("first-upload: ")
    for value in ("secret-bucket-key", "AKIDCONFIG"):
        assert value not in captured.err
    return lines[0]


@pytest.mark.parametrize(
    "now", [SUNDAY_1950.replace(minute=55), SUNDAY_20, SUNDAY_2330.replace(minute=29)]
)
def test_it_refuses_inside_the_sunday_scrub_window(tmp_path, monkeypatch, capsys, now):
    _, config = _setup(tmp_path)
    client = FakeS3()
    with pytest.raises(SystemExit) as exited:
        _main(config, client, monkeypatch, now=now)
    assert "19:55 to 23:30" in _refused(capsys, exited)
    assert client.calls == []


def test_a_run_begun_before_the_sunday_window_stops_when_it_opens(tmp_path, monkeypatch, capsys):
    # Begun at 19:50, each PUT takes three minutes, so the window opens partway through.
    # The check runs before every request, so the run stops with one line, and the
    # bucket is left with no manifest.jsonl for the scrub to find a watermark in.
    _, config = _setup(tmp_path)
    clock = ManualClock(SUNDAY_1950)
    client = FakeS3(on_put=lambda kwargs, data: clock.advance(3 * 60))
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)

    with pytest.raises(SystemExit) as exited:
        bucket.main(
            ["first-upload", "--config", str(config), "--target", TARGET],
            clock=clock,
            calendar=CALENDAR,
        )

    line = _refused(capsys, exited)
    assert "19:55 to 23:30" in line and "stopped after 2 PUT(s)" in line
    assert len(client.puts()) == 2
    assert "lake/manifest.jsonl" not in client.keys()


@pytest.mark.parametrize("now", [SUNDAY_1950, SUNDAY_2330])
def test_it_runs_either_side_of_the_sunday_window(tmp_path, monkeypatch, now):
    _, config = _setup(tmp_path)
    assert _main(config, FakeS3(), monkeypatch, now=now) == 0


def test_a_path_target_with_no_target_flag_refuses(tmp_path, monkeypatch, capsys):
    _, config = _setup(tmp_path)
    with pytest.raises(SystemExit) as exited:
        _main(config, FakeS3(), monkeypatch, target=None)
    assert "--target" in _refused(capsys, exited)


def test_a_config_without_the_bucket_keys_refuses_naming_them(tmp_path, monkeypatch, capsys):
    # The real client builder runs here and refuses before it imports anything.
    _, config = _setup(tmp_path, keys=False)
    with pytest.raises(SystemExit) as exited:
        _main(config, None, monkeypatch)
    assert "bucket_secret_access_key" in _refused(capsys, exited)


def test_an_unreachable_bucket_refuses_with_one_line(tmp_path, monkeypatch, capsys):
    _, config = _setup(tmp_path)
    client = FakeS3()
    client.fail_with = unreachable()
    with pytest.raises(SystemExit) as exited:
        _main(config, client, monkeypatch)
    line = _refused(capsys, exited)
    assert "could not be reached or was unavailable (EndpointConnectionError)" in line
    assert "the bucket's credentials" not in line


@pytest.mark.parametrize(
    ("code", "status", "named"),
    [
        (
            "AccessDenied",
            403,
            "refused the request (AccessDenied), so the bucket's credentials or their policy",
        ),
        ("SlowDown", 503, "could not be reached or was unavailable (SlowDown)"),
        ("InternalError", 500, "could not be reached or was unavailable (InternalError)"),
        ("NoSuchBucket", 404, "answered with an error (NoSuchBucket)"),
    ],
)
def test_each_bucket_failure_gets_its_own_line(tmp_path, monkeypatch, capsys, code, status, named):
    _, config = _setup(tmp_path)
    client = FakeS3()
    client.fail_with = client_error(code, "HeadObject", status)
    with pytest.raises(SystemExit) as exited:
        _main(config, client, monkeypatch)
    line = _refused(capsys, exited)
    assert named in line
    if code != "AccessDenied":
        assert "the bucket's credentials" not in line


def test_a_rotted_file_refuses_with_one_line_and_leaves_the_manifest_out(
    tmp_path, monkeypatch, capsys
):
    lake, config = _setup(tmp_path)
    next(lake.glob("chains/**/*.parquet")).write_bytes(b"rot")
    client = FakeS3()
    with pytest.raises(SystemExit) as exited:
        _main(config, client, monkeypatch)
    line = _refused(capsys, exited)
    assert "no longer match its manifest entry" in line
    # The benign causes are named beside rot, so a race does not read as decay.
    for cause in ("Rot", "recompact", "journal segment", "compaction runs by hand"):
        assert cause in line
    assert "after it was sealed" not in line
    assert "lake/manifest.jsonl" not in client.keys()


def test_a_bug_is_not_swallowed(tmp_path, monkeypatch):
    _, config = _setup(tmp_path)
    client = FakeS3()
    client.fail_with = ZeroDivisionError("a real bug")
    with pytest.raises(ZeroDivisionError):
        _main(config, client, monkeypatch)
