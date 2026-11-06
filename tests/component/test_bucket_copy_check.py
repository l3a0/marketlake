"""The vendor sweep's page while captured data stays off the bucket (marketlake #996).

A failed upload pages once, on the transition of the ``compaction`` and ``evening-upload``
checks, and a check that stays down sends nothing more. The live copy of a quotes segment now
lasts about 97 days, and chains have none, so a sealed day that stays off the bucket can end
with the VM's volume as its only copy. This file drives the page that keeps saying so.

The first test is the issue's own: a run whose upload lands no PUT, then the sweep on each
following weekday, with a page each time and none once the upload clears. It goes through the
real ``compact`` and the real ``sweep`` against ``FakeS3``, so removing the check fails it. The
rest pin what that run cannot reach: what counts as owed, which day counts as late, and what
happens when the check cannot read.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from lake import bucket, sweep
from lake.alert import Publisher
from lake.bucket import (
    BucketBackup,
    CaptureOwed,
    capture_owed,
    first_upload,
)
from lake.compact import compact
from lake.config import BucketTarget, load_config
from lake.manifest import append_manifest
from lake.report import ACTION, HEALTHY, ReportLines
from lake.sweep import (
    BUCKET_BEHIND_EVENT,
    BUCKET_BEHIND_TITLE,
    BUCKET_REPAIR,
    BUCKET_UNCHECKED_EVENT,
    BUCKET_UNCHECKED_TITLE,
    HOLIDAY_BODY,
)
from tests.component.test_compaction import _chains, _segment, _snap
from tests.component.test_eod_sweep import EVENING, FOLLOWING, MONDAY, NEXT_MONDAY, _lake, _run
from tests.support.bucket import FakeS3, client_error, unreachable
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.lake import FixtureLake
from tests.support.pinger import FakePinger
from tests.support.transport import FakeTransport

TARGET = BucketTarget(bucket="lake-backup", prefix="lake")
PING = "https://hc-ping.com/secret-key/compaction"
BUCKET_KEYS = (
    "bucket_access_key_id: AKIDCONFIG\n"
    "bucket_secret_access_key: secret-bucket-key\n"
    "bucket_region: us-east-2\n"
)

THURSDAY = date(2026, 9, 17)
WEDNESDAY = date(2026, 9, 16)
EARLIER = date(2026, 9, 11)


def _et(day: date, hour: int, minute: int) -> datetime:
    return datetime.fromisoformat(f"{day.isoformat()}T{hour:02d}:{minute:02d}:00-04:00")


def _evening(day: date) -> datetime:
    return _et(day, 18, 30)


def _calendar(holidays: tuple[date, ...] = ()):
    return weekday_sessions(MONDAY, NEXT_MONDAY, holidays=holidays)


def _seed(root: Path) -> FakeS3:
    """A bucket whose manifest copy reaches everything the lake holds now."""
    client = FakeS3()
    first_upload(
        root,
        TARGET,
        client=client,
        clock=ManualClock(_evening(MONDAY)),
        calendar=_calendar(),
    )
    client.calls.clear()
    return client


def _seeded_lake(lake_root: Path) -> FakeS3:
    FixtureLake(lake_root).with_chains("SPY", EARLIER).build()
    return _seed(lake_root)


def _owe(root: Path, rel: str) -> None:
    """Record a manifest entry past the bucket's copy. ``capture_owed`` reads no lake file."""
    append_manifest(
        root,
        partition=rel,
        source="capture",
        sha256="0" * 64,
        rows=1,
        fetched_at=None,
        guard=False,
    )


def _chains_rel(day: date, ticker: str = "SPY") -> str:
    return f"chains/ticker={ticker}/date={day.isoformat()}.parquet"


def _quotes_rel(day: date, ticker: str = "SPY") -> str:
    return f"quotes/ticker={ticker}/date={day.isoformat()}.parquet"


def _segment_rel(day: date, surface: str = "chains", ticker: str = "SPY") -> str:
    return f"journal/date={day.isoformat()}/surface={surface}/ticker={ticker}/seg-a-1.arrows"


def _source(client: FakeS3):
    return lambda: (TARGET, client)


def _pages(transport: FakeTransport, event: str = BUCKET_BEHIND_EVENT) -> list:
    return [message for message in transport.messages if message.event == event]


def _compact(root: Path, client: FakeS3, day: date, hour: int, minute: int, pinger: FakePinger):
    clock = ManualClock(_et(day, hour, minute))
    calendar = _calendar()
    return compact(
        root,
        clock=clock,
        calendar=calendar,
        backup=BucketBackup(client=client, clock=clock, calendar=calendar),
        backup_target=TARGET,
        pinger=pinger,
        ping_url=PING,
        plan_path=root.parent / "chain_plan.json",
    )


def _refuse_every_put(kwargs, data) -> None:
    raise client_error("AccessDenied", "PutObject", 403)


# -- the issue's run ----------------------------------------------------------


def test_an_upload_that_lands_no_put_pages_each_weekday_until_the_upload_clears_it(
    fixture_lake: FixtureLake,
):
    """The done-when of marketlake #996, through the real compaction and the real sweep.

    ``FOLLOWING`` seals with every PUT refused, so nothing about it reaches the bucket and the
    ``compaction`` ping is withheld. That session's own evening sends nothing yet, because the
    check it missed is the page for that night. Each weekday after it pages once. The upload
    then lands, and the next evening is silent and says so in the report.
    """
    root = _lake(fixture_lake)
    client = _seed(root)
    _segment(
        root, "chains", "SPY", FOLLOWING, _chains(2, snap_ts=_snap(FOLLOWING, 0)), start_ts="a"
    )
    client.on_put = _refuse_every_put

    pinger = FakePinger()
    with pytest.raises(ClientError):
        _compact(root, client, FOLLOWING, 16, 30, pinger)
    assert pinger.urls == []
    assert any(key.endswith("manifest.jsonl") for key in client.objects)
    assert (root / _chains_rel(FOLLOWING)).exists()

    _, _, first_night = _run(root, now=_evening(FOLLOWING), bucket_source=_source(client))
    assert _pages(first_night) == []

    for day in (WEDNESDAY, THURSDAY, date(2026, 9, 18), NEXT_MONDAY):
        outcome, night_pinger, transport = _run(
            root, now=_evening(day), bucket_source=_source(client)
        )
        (page,) = _pages(transport)
        assert page.title == BUCKET_BEHIND_TITLE
        assert page.priority == 5
        assert f"1 captured file off the bucket, oldest {FOLLOWING.isoformat()}" in page.body
        assert BUCKET_REPAIR in page.body
        # The page never touches the ping or the hand-off, and the hand-off is the upload that
        # may clear it.
        assert night_pinger.urls != []
        assert outcome.hand_off is True

    client.on_put = None
    _compact(root, client, NEXT_MONDAY, 16, 30, FakePinger())
    outcome, _, transport = _run(root, now=_evening(NEXT_MONDAY), bucket_source=_source(client))
    assert _pages(transport) == []
    assert _pages(transport, BUCKET_UNCHECKED_EVENT) == []
    assert any(
        line.startswith("bucket copy current") and kind == HEALTHY
        for line, kind in zip(outcome.nightly.report, outcome.nightly.report_kinds, strict=True)
    )


# -- what counts as owed ------------------------------------------------------


def test_sealed_chains_and_quotes_past_the_watermark_are_owed_oldest_first(lake_root):
    client = _seeded_lake(lake_root)
    _owe(lake_root, _quotes_rel(THURSDAY))
    _owe(lake_root, _chains_rel(WEDNESDAY))

    owed = capture_owed(lake_root, TARGET, client)

    assert owed == CaptureOwed(
        files=((WEDNESDAY, _chains_rel(WEDNESDAY)), (THURSDAY, _quotes_rel(THURSDAY)))
    )


def test_what_the_bucket_copy_reaches_is_not_owed(lake_root):
    client = _seeded_lake(lake_root)
    assert capture_owed(lake_root, TARGET, client) == CaptureOwed()


def test_a_partition_rewritten_past_the_watermark_is_owed(lake_root):
    """The upload's own rule. An entry the copy reaches and a newer one it does not."""
    client = _seeded_lake(lake_root)
    _owe(lake_root, _chains_rel(EARLIER))

    assert capture_owed(lake_root, TARGET, client).files == ((EARLIER, _chains_rel(EARLIER)),)


def test_bars_and_ledgers_are_not_captured_data(lake_root):
    """A vendor can supply them again, so they add nothing to a page about captured minutes."""
    client = _seeded_lake(lake_root)
    _owe(lake_root, f"bars/ticker=SPY/freq=1d/date={WEDNESDAY.isoformat()}.parquet")
    _owe(lake_root, "quarantine.jsonl")
    _owe(lake_root, "reference/capture_spans.parquet")

    assert capture_owed(lake_root, TARGET, client) == CaptureOwed()


def test_a_segment_of_a_day_that_never_sealed_is_owed(lake_root):
    """A seal that raises on every run seals nothing, so no partition exists to count.

    The day's segments are then what the upload owes. Reading partitions alone would leave
    the page silent on the night the most is at stake.
    """
    client = _seeded_lake(lake_root)
    _owe(lake_root, _segment_rel(WEDNESDAY))
    _owe(lake_root, _segment_rel(WEDNESDAY, "quotes"))

    assert [day for day, _ in capture_owed(lake_root, TARGET, client).files] == [
        WEDNESDAY,
        WEDNESDAY,
    ]


def test_a_segment_whose_partition_sealed_is_not_owed(lake_root):
    """Its entry stays in the manifest after the seal deletes its file, on every healthy day."""
    client = _seeded_lake(lake_root)
    _owe(lake_root, _segment_rel(WEDNESDAY))
    _owe(lake_root, _chains_rel(WEDNESDAY))

    assert capture_owed(lake_root, TARGET, client).files == ((WEDNESDAY, _chains_rel(WEDNESDAY)),)


def test_a_segment_of_another_surface_is_not_owed(lake_root):
    client = _seeded_lake(lake_root)
    _owe(lake_root, _segment_rel(WEDNESDAY, "bars"))

    assert capture_owed(lake_root, TARGET, client) == CaptureOwed()


def test_the_check_reads_the_manifest_copy_and_no_other_object(lake_root):
    client = _seeded_lake(lake_root)
    _owe(lake_root, _chains_rel(WEDNESDAY))

    capture_owed(lake_root, TARGET, client)

    assert [(name, kwargs["Key"]) for name, kwargs in client.calls] == [
        ("head_object", "lake/manifest.jsonl")
    ]


def test_a_copy_with_no_stored_checksum_is_read_to_find_where_it_ends(lake_root):
    """The scrub's own fallback, so an older object cannot read as a copy that parted."""
    client = _seeded_lake(lake_root)
    held = client.body("lake/manifest.jsonl")
    client.objects.clear()
    client.store("lake/manifest.jsonl", held)
    _owe(lake_root, _chains_rel(WEDNESDAY))

    assert capture_owed(lake_root, TARGET, client).files == ((WEDNESDAY, _chains_rel(WEDNESDAY)),)


# -- when it cannot answer ----------------------------------------------------


def test_an_empty_lake_owes_nothing_and_sends_no_request(lake_root):
    client = FakeS3()

    assert capture_owed(lake_root, TARGET, client) == CaptureOwed()
    assert client.calls == []


def test_a_bucket_with_no_manifest_copy_is_unknown_rather_than_healthy(lake_root):
    FixtureLake(lake_root).with_chains("SPY", EARLIER).build()

    assert capture_owed(lake_root, TARGET, FakeS3()) == CaptureOwed(
        unknown="the bucket holds no usable manifest copy"
    )


def test_a_copy_holding_no_whole_entry_is_unknown(lake_root):
    client = _seeded_lake(lake_root)
    client.objects.clear()
    client.store("lake/manifest.jsonl", b"")

    assert capture_owed(lake_root, TARGET, client).unknown == (
        "the bucket holds no usable manifest copy"
    )


def test_a_copy_that_parted_from_the_lake_is_unknown(lake_root):
    client = _seeded_lake(lake_root)
    client.objects.clear()
    client.store("lake/manifest.jsonl", b'{"partition": "other"}\n')

    owed = capture_owed(lake_root, TARGET, client)

    assert owed.files == ()
    assert owed.unknown == "the bucket's manifest copy parted from the lake's at byte 2"


@pytest.mark.parametrize(
    ("error", "named"),
    [
        (client_error("AccessDenied", "HeadObject", 403), "bucket refused (AccessDenied)"),
        (client_error("NoSuchBucket", "HeadObject", 400), "bucket failed (NoSuchBucket)"),
        (unreachable(), "bucket unreachable (EndpointConnectionError)"),
    ],
)
def test_a_bucket_that_will_not_answer_is_unknown_and_named(lake_root, error, named):
    client = _seeded_lake(lake_root)
    client.fail_with = error

    assert capture_owed(lake_root, TARGET, client) == CaptureOwed(unknown=named)


def test_a_bug_in_the_read_raises_rather_than_reading_as_a_bucket_failure(lake_root):
    FixtureLake(lake_root).with_chains("SPY", EARLIER).build()

    with pytest.raises(AttributeError):
        capture_owed(lake_root, TARGET, object())


# -- which day counts as late -------------------------------------------------


def test_a_file_from_tonights_session_does_not_page(fixture_lake: FixtureLake, lake_root):
    root = _lake(fixture_lake)
    client = _seed(root)
    _owe(root, _chains_rel(MONDAY, "QQQ"))

    outcome, _, transport = _run(root, now=EVENING, bucket_source=_source(client))

    assert _pages(transport) == []
    assert outcome.nightly.report[0] == f"bucket copy current: nothing owed from before {MONDAY}"


def test_a_file_from_the_session_before_tonights_pages(fixture_lake: FixtureLake):
    root = _lake(fixture_lake)
    client = _seed(root)
    _owe(root, _chains_rel(EARLIER, "QQQ"))

    outcome, _, transport = _run(root, now=EVENING, bucket_source=_source(client))

    assert len(_pages(transport)) == 1
    assert outcome.nightly.report[0] == f"bucket copy behind: 1 captured file, oldest {EARLIER}"
    assert outcome.nightly.report_kinds[0] == ACTION


def test_the_oldest_day_and_the_count_read_across_files(fixture_lake: FixtureLake):
    root = _lake(fixture_lake)
    client = _seed(root)
    _owe(root, _quotes_rel(date(2026, 9, 10), "QQQ"))
    _owe(root, _chains_rel(EARLIER, "QQQ"))
    _owe(root, _segment_rel(date(2026, 9, 9), ticker="IWM"))
    # Tonight's file is owed and is not late, so it joins neither number.
    _owe(root, _chains_rel(MONDAY, "QQQ"))

    _, _, transport = _run(root, now=EVENING, bucket_source=_source(client))

    (page,) = _pages(transport)
    assert "3 captured files off the bucket, oldest 2026-09-09" in page.body


def test_a_catch_up_morning_measures_from_the_session_it_follows(fixture_lake: FixtureLake):
    """Before tonight's compaction moment the last compacted session is the one before."""
    root = _lake(fixture_lake)
    client = _seed(root)
    _owe(root, _chains_rel(MONDAY, "QQQ"))
    morning = _et(FOLLOWING, 8, 25)

    _, _, transport = _run(root, now=morning, bucket_source=_source(client))
    assert _pages(transport) == []

    _owe(root, _chains_rel(EARLIER, "QQQ"))
    _, _, transport = _run(root, now=morning + timedelta(minutes=1), bucket_source=_source(client))
    assert len(_pages(transport)) == 1


def test_a_holiday_pages_and_measures_from_the_session_before_it(fixture_lake: FixtureLake):
    root = _lake(fixture_lake)
    client = _seed(root)
    _owe(root, _chains_rel(FOLLOWING, "QQQ"))
    holiday = _evening(WEDNESDAY)

    outcome, _, transport = _run(
        root, now=holiday, holidays=(WEDNESDAY,), bucket_source=_source(client)
    )
    assert _pages(transport) == []
    assert outcome.digest.body == HOLIDAY_BODY

    _owe(root, _chains_rel(MONDAY, "QQQ"))
    outcome, _, transport = _run(
        root,
        now=holiday + timedelta(minutes=1),
        holidays=(WEDNESDAY,),
        bucket_source=_source(client),
    )
    assert len(_pages(transport)) == 1
    assert outcome.digest.body == HOLIDAY_BODY


def test_a_calendar_naming_no_recent_session_pages_early_rather_than_going_quiet(
    fixture_lake: FixtureLake,
):
    """The fallback bar is today, so the failure of the search reads loud."""
    root = _lake(fixture_lake)
    client = _seed(root)
    _owe(root, _chains_rel(EARLIER, "QQQ"))
    # A Saturday in a week the calendar does not list, three weeks past its last session.
    far = _evening(date(2026, 10, 17))

    _, _, transport = _run(root, now=far, bucket_source=_source(client))

    assert len(_pages(transport)) == 1


# -- when the check itself fails ----------------------------------------------


def test_a_bucket_that_will_not_answer_pages_and_costs_the_run_nothing(fixture_lake: FixtureLake):
    root = _lake(fixture_lake)
    client = _seed(root)
    client.fail_with = client_error("AccessDenied", "HeadObject", 403)

    outcome, pinger, transport = _run(root, now=EVENING, bucket_source=_source(client))

    (page,) = _pages(transport, BUCKET_UNCHECKED_EVENT)
    assert page.title == BUCKET_UNCHECKED_TITLE
    assert page.priority == 5
    assert "bucket refused (AccessDenied)" in page.body
    assert BUCKET_REPAIR in page.body
    assert _pages(transport) == []
    assert pinger.urls != []
    assert outcome.hand_off is True
    assert outcome.nightly.report[0] == "bucket copy not checked: bucket refused (AccessDenied)"
    assert outcome.nightly.report_kinds[0] == ACTION


def test_a_source_that_raises_pages_by_type_alone(fixture_lake: FixtureLake):
    """A refused bucket setting lands here, and its message may quote a config value."""
    root = _lake(fixture_lake)

    def refused():
        raise bucket.BucketSettingsInvalid("bucket_region secret-value is not a region")

    outcome, pinger, transport = _run(root, now=EVENING, bucket_source=refused)

    (page,) = _pages(transport, BUCKET_UNCHECKED_EVENT)
    assert "BucketSettingsInvalid" in page.body
    assert "secret-value" not in page.body
    assert pinger.urls != []
    assert outcome.hand_off is True


def test_a_damaged_lake_manifest_pages_rather_than_raising(lake_root):
    client = _seeded_lake(lake_root)
    with (lake_root / "manifest.jsonl").open("a") as handle:
        handle.write('{"source": "names no partition"}\n')
    transport = FakeTransport()
    lines = ReportLines()

    sweep._check_bucket_copy(
        lake_root,
        bucket_source=_source(client),
        now=EVENING,
        calendar=_calendar(),
        publisher=Publisher(lake_root=lake_root, transport=transport),
        report=lines,
    )

    assert lines.lines == ("bucket copy not checked: ManifestError",)
    assert len(_pages(transport, BUCKET_UNCHECKED_EVENT)) == 1


def test_no_bucket_source_sends_nothing_and_files_no_line(fixture_lake: FixtureLake):
    root = _lake(fixture_lake)

    outcome, _, transport = _run(root, now=EVENING)

    assert _pages(transport) == []
    assert _pages(transport, BUCKET_UNCHECKED_EVENT) == []
    assert not any("bucket copy" in line for line in outcome.nightly.report)


def test_the_page_goes_out_before_any_walk_can_raise(fixture_lake: FixtureLake, monkeypatch):
    """The runway check's reason: a raise that loses the report file must not lose the page."""
    root = _lake(fixture_lake)
    client = _seed(root)
    _owe(root, _chains_rel(EARLIER, "QQQ"))

    def explode(**kwargs):
        raise RuntimeError("a walk raised past its containment")

    monkeypatch.setattr(sweep, "extract_dividends", explode)
    transport = FakeTransport()
    with pytest.raises(RuntimeError):
        _run(root, now=EVENING, transport=transport, bucket_source=_source(client))

    assert len(_pages(transport)) == 1


def test_the_body_carries_no_path_or_bucket_name(fixture_lake: FixtureLake):
    root = _lake(fixture_lake)
    client = _seed(root)
    _owe(root, _chains_rel(EARLIER, "QQQ"))

    _, _, transport = _run(root, now=EVENING, bucket_source=_source(client))

    (page,) = _pages(transport)
    assert "lake-backup" not in page.body
    assert "ticker=" not in page.body
    assert "s3://" not in page.body


# -- the wiring ---------------------------------------------------------------


def _bucket_config(tmp_path: Path, lake_root: Path, **kwargs) -> Path:
    config = write_config(tmp_path, lake_root, **kwargs)
    text = config.read_text().replace(
        f"backup_target: {tmp_path / 'ssd'}", f"backup_target: {TARGET}"
    )
    config.write_text(text + BUCKET_KEYS)
    return config


def test_a_primary_with_a_bucket_target_gets_a_source_that_connects_on_call(
    tmp_path, lake_root, monkeypatch
):
    config = load_config(_bucket_config(tmp_path, lake_root))
    connected: list[object] = []
    monkeypatch.setattr(bucket, "connect", lambda given: connected.append(given) or (TARGET, None))

    source = sweep.bucket_source_from_config(config, "primary")

    # Built here it would end the run inside ``input_errors_exit`` on a refused setting.
    assert connected == []
    assert source is not None
    assert source() == (TARGET, None)
    assert connected == [config]


def test_a_shadow_host_gets_no_source(tmp_path, lake_root):
    config = load_config(_bucket_config(tmp_path, lake_root))

    assert sweep.bucket_source_from_config(config, "shadow") is None


def test_a_path_target_gets_no_source(tmp_path, lake_root):
    config = load_config(write_config(tmp_path, lake_root))

    assert sweep.bucket_source_from_config(config, "primary") is None


def test_the_evening_run_carries_the_source_from_the_config(tmp_path, lake_root, monkeypatch):
    path = _bucket_config(tmp_path, lake_root)
    seen: dict[str, object] = {}
    monkeypatch.setattr(sweep, "sweep", lambda **kwargs: seen.update(kwargs))
    monkeypatch.setattr(sweep, "load_tickers", lambda _path: None)

    sweep.sweep_from_config(config_path=path)

    assert callable(seen["bucket_source"])


def test_the_hand_check_pages_and_exits_one_when_files_are_late(
    tmp_path, lake_root, monkeypatch, capsys
):
    client = _seeded_lake(lake_root)
    _owe(lake_root, _chains_rel(EARLIER, "QQQ"))
    path = _bucket_config(tmp_path, lake_root)
    transport = FakeTransport()
    monkeypatch.setattr(bucket, "connect", lambda config: (TARGET, client))
    monkeypatch.setattr("lake.alert.NtfyTransport", lambda topic: transport)
    pinger = FakePinger()
    monkeypatch.setattr("lake.runner.UrllibPinger", lambda: pinger)

    code = sweep.main(
        ["--config", str(path), "--bucket-check"],
        clock=ManualClock(_evening(NEXT_MONDAY)),
    )

    assert code == 1
    assert len(_pages(transport)) == 1
    assert pinger.urls == []
    assert f"bucket copy behind: 1 captured file, oldest {EARLIER}" in capsys.readouterr().out


def test_the_hand_check_sends_nothing_and_exits_zero_on_a_current_bucket(
    tmp_path, lake_root, monkeypatch, capsys
):
    client = _seeded_lake(lake_root)
    path = _bucket_config(tmp_path, lake_root)
    transport = FakeTransport()
    monkeypatch.setattr(bucket, "connect", lambda config: (TARGET, client))
    monkeypatch.setattr("lake.alert.NtfyTransport", lambda topic: transport)

    code = sweep.main(
        ["--config", str(path), "--bucket-check"],
        clock=ManualClock(_evening(NEXT_MONDAY)),
    )

    assert code == 0
    assert transport.messages == []
    assert "bucket copy current" in capsys.readouterr().out


def test_the_hand_check_on_a_shadow_says_so_and_never_connects(
    tmp_path, lake_root, monkeypatch, capsys
):
    path = _bucket_config(tmp_path, lake_root, role="shadow")

    def refuse(config):
        raise AssertionError("a shadow host connected to the bucket")

    monkeypatch.setattr(bucket, "connect", refuse)

    code = sweep.main(
        ["--config", str(path), "--bucket-check"],
        clock=ManualClock(_evening(NEXT_MONDAY)),
    )

    assert code == 0
    assert "no bucket copy is owed" in capsys.readouterr().out


# -- the runbook --------------------------------------------------------------


def _runbook() -> str:
    return (Path(__file__).parents[2] / "infra" / "README.md").read_text()


def test_the_pages_send_the_operator_to_a_heading_that_exists():
    place, heading = BUCKET_REPAIR.split(", ", 1)

    assert place == "infra/README.md"
    assert f"### {heading}\n" in _runbook()


def test_the_drills_scratch_manifest_line_pages_and_leaves_the_real_lake_alone(
    tmp_path, lake_root, monkeypatch, capsys
):
    """Part 1 of the drill, by the line the runbook prints.

    The manifest copy gets the runbook's own line, and a config copy points at the scratch
    directory. The bucket's copy stays a prefix of it, so the file reads as owed by the same
    read the evening runs, and nothing under the real lake root changes.
    """
    client = _seeded_lake(lake_root)
    seeded = len(client.puts())
    before = (lake_root / "manifest.jsonl").read_bytes()
    line = re.search(r"echo '(\{.*?\})' >>", _runbook()).group(1)
    assert json.loads(line)["partition"].startswith("chains/")

    scratch = tmp_path / "scratch"
    scratch.mkdir()
    (scratch / "manifest.jsonl").write_bytes(before + line.encode() + b"\n")
    path = _bucket_config(tmp_path, lake_root)
    copy = scratch / "config.yaml"
    copy.write_text(path.read_text().replace(f"lake_root: {lake_root}", f"lake_root: {scratch}"))
    transport = FakeTransport()
    monkeypatch.setattr(bucket, "connect", lambda config: (TARGET, client))
    monkeypatch.setattr("lake.alert.NtfyTransport", lambda topic: transport)

    code = sweep.main(
        ["--config", str(copy), "--bucket-check"],
        clock=ManualClock(_evening(NEXT_MONDAY)),
    )

    assert code == 1
    (page,) = _pages(transport)
    assert "1 captured file off the bucket, oldest 2026-01-02" in page.body
    assert "bucket copy behind: 1 captured file, oldest 2026-01-02" in capsys.readouterr().out
    assert (lake_root / "manifest.jsonl").read_bytes() == before
    assert len(client.puts()) == seeded
