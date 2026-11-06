"""The vendor sweep's page while captured data stays off the bucket (marketlake #996).

A failed upload pages on the transition of the ``compaction`` and ``evening-upload`` checks,
and nothing in this repository repeats that page. The live copy of a quotes segment now lasts
about 97 days, and chains have none, so a sealed day that stays off the bucket can end with
the VM's volume as its only copy. This file drives the page that keeps saying so.

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
from lake.manifest import LedgerNotUtf8, TornLedger, append_manifest
from lake.paths import LakePaths
from lake.report import ACTION, HEALTHY, INFO, ReportLines
from lake.sweep import (
    BUCKET_BEHIND_EVENT,
    BUCKET_BEHIND_TITLE,
    BUCKET_REPAIR,
    BUCKET_UNCHECKED_EVENT,
    BUCKET_UNCHECKED_TITLE,
    HOLIDAY_BODY,
)
from tests.component.test_compaction import _chains, _segment, _snap
from tests.component.test_eod_sweep import (
    EVENING,
    FOLLOWING,
    MONDAY,
    NEXT_MONDAY,
    _lake,
    _run,
    _stub_free,
)
from tests.support.bucket import FakeS3, client_error, unreachable
from tests.support.calendar import FakeCalendar, SessionTimes, weekday_sessions
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
WEEK_BEFORE = date(2026, 9, 7)


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
    assert ("bucket copy current", HEALTHY) in zip(
        outcome.nightly.report, outcome.nightly.report_kinds, strict=True
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
    the page silent on the night the most is at stake. The paths come from the builder capture
    writes through, so a change to their shape reaches this test.
    """
    client = _seeded_lake(lake_root)
    paths = LakePaths(lake_root)
    for surface in ("chains", "quotes"):
        built = paths.segment_path(surface, "SPY", WEDNESDAY, "a", 1)
        _owe(lake_root, built.relative_to(lake_root).as_posix())

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


@pytest.mark.parametrize(
    "rel",
    [
        "chains/ticker=SPY/date=2026-13-45.parquet",
        "journal/date=2026-13-45/surface=chains/ticker=SPY/seg-a-1.arrows",
    ],
    ids=["partition", "segment"],
)
def test_a_path_that_will_not_parse_is_left_to_the_stuck_watermark(lake_root, rel):
    """No writer makes one, so the choice is to read past it rather than page on it.

    The upload owes it all the same, so a failed upload stops the watermark there, and the next
    session's files page.
    """
    client = _seeded_lake(lake_root)
    _owe(lake_root, rel)

    assert capture_owed(lake_root, TARGET, client) == CaptureOwed()


def test_a_bars_entry_past_the_watermark_reads_as_current(lake_root):
    """The line speaks for captured files, and a stuck upload pages through the next chains."""
    client = _seeded_lake(lake_root)
    _owe(lake_root, f"bars/ticker=SPY/freq=1d/date={EARLIER.isoformat()}.parquet")

    _, lines = _checked(lake_root, client, _evening(NEXT_MONDAY), _calendar())

    assert lines == ("bucket copy current",)


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


def test_an_empty_lake_and_an_empty_bucket_owe_nothing(lake_root):
    assert capture_owed(lake_root, TARGET, FakeS3()) == CaptureOwed()


def test_an_empty_lake_and_a_zero_byte_copy_owe_nothing(lake_root):
    """A copy holding no whole entry reads as absent only while the lake holds some."""
    client = FakeS3()
    client.store("lake/manifest.jsonl", b"")

    assert capture_owed(lake_root, TARGET, client) == CaptureOwed()


@pytest.mark.parametrize("gone", [True, False], ids=["absent", "emptied"])
def test_a_lake_that_lost_its_manifest_is_unknown_rather_than_healthy(lake_root, gone):
    """An unmounted volume or a lost manifest, with the bucket still holding a full copy."""
    client = _seeded_lake(lake_root)
    if gone:
        (lake_root / "manifest.jsonl").unlink()
    else:
        (lake_root / "manifest.jsonl").write_bytes(b"")

    assert capture_owed(lake_root, TARGET, client).unknown == (
        "the bucket's manifest copy parted from the lake's at byte 0"
    )


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
    _owe(root, _chains_rel(MONDAY, "IWM"))

    outcome, _, transport = _run(root, now=EVENING, bucket_source=_source(client))

    assert _pages(transport) == []
    assert outcome.nightly.report[0] == f"bucket copy owes 2 from {MONDAY}"
    assert outcome.nightly.report_kinds[0] == INFO


def test_a_file_newer_than_the_last_compacted_session_is_named_by_its_own_day(lake_root):
    """A hand check inside a session owes that session's segments, not the last session's."""
    client = _seeded_lake(lake_root)
    _owe(lake_root, _segment_rel(FOLLOWING))

    pages, lines = _checked(lake_root, client, _et(FOLLOWING, 12, 0), _calendar())

    assert pages == []
    assert lines == (f"bucket copy owes 1 from {FOLLOWING}",)


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


def _checked(
    root: Path, client: FakeS3, now: datetime, calendar, event: str = BUCKET_BEHIND_EVENT
) -> tuple[list, tuple]:
    """The check alone, for the cases about the clock, which the whole sweep adds nothing to."""
    transport = FakeTransport()
    lines = ReportLines()
    sweep._check_bucket_copy(
        root,
        bucket_source=_source(client),
        now=now,
        calendar=calendar,
        publisher=Publisher(lake_root=root, transport=transport),
        report=lines,
    )
    return _pages(transport, event), lines.lines


@pytest.mark.parametrize(
    ("minute", "pages"),
    [(29, 0), (30, 1), (31, 1)],
    ids=["a minute before the moment", "at the moment", "a minute after"],
)
def test_a_session_counts_from_its_close_plus_fifteen_minutes(lake_root, minute, pages):
    """The design's moment, the option close at 16:15 plus 15 minutes, written as literals.

    Before it the last compacted session is Friday, so a Friday file is not late yet. From it
    the last compacted session is Monday, so the same file is.
    """
    client = _seeded_lake(lake_root)
    _owe(lake_root, _chains_rel(EARLIER, "QQQ"))
    calendar = weekday_sessions(WEEK_BEFORE, MONDAY)

    page_list, _ = _checked(lake_root, client, _et(MONDAY, 16, minute), calendar)

    assert len(page_list) == pages


def test_a_monday_holiday_reaches_back_over_the_weekend(lake_root):
    """Friday is the session before it, three days back, so a Friday file is not late yet."""
    client = _seeded_lake(lake_root)
    calendar = weekday_sessions(WEEK_BEFORE, MONDAY, holidays=(MONDAY,))
    _owe(lake_root, _chains_rel(EARLIER, "QQQ"))

    page_list, _ = _checked(lake_root, client, _evening(MONDAY), calendar)
    assert page_list == []

    _owe(lake_root, _chains_rel(date(2026, 9, 10), "QQQ"))
    page_list, _ = _checked(lake_root, client, _evening(MONDAY), calendar)
    assert len(page_list) == 1


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


@pytest.mark.parametrize(
    ("days_back", "reason"),
    [(13, None), (14, "LookupError")],
    ids=["the last day the walk reaches", "the first day past it"],
)
def test_the_walk_reaches_thirteen_days_back_and_then_says_it_could_not_answer(
    lake_root, days_back, reason
):
    client = _seeded_lake(lake_root)
    today = date(2026, 9, 25)
    session = today - timedelta(days=days_back)
    _owe(lake_root, _chains_rel(session, "QQQ"))
    calendar = FakeCalendar(
        {session: SessionTimes(open=_et(session, 9, 30), close=_et(session, 16, 0))}
    )

    pages, lines = _checked(lake_root, client, _evening(today), calendar, BUCKET_UNCHECKED_EVENT)

    if reason is None:
        assert pages == []
        assert lines == (f"bucket copy owes 1 from {session}",)
    else:
        assert len(pages) == 1
        assert lines == (f"bucket copy not checked: {reason}",)


def test_a_calendar_that_raises_stays_inside_the_containment(lake_root):
    """A raise here would take the digest, the ping and the hand-off with it."""

    class Refusing:
        def is_session(self, day):
            raise RuntimeError("past the calendar's end")

    client = _seeded_lake(lake_root)

    pages, lines = _checked(
        lake_root, client, _evening(NEXT_MONDAY), Refusing(), BUCKET_UNCHECKED_EVENT
    )

    assert len(pages) == 1
    assert lines == ("bucket copy not checked: RuntimeError",)


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


def _torn_with_entries_behind(root: Path) -> None:
    """A fragment with one entry fused onto it, then a whole entry no read gets past."""
    with (root / "manifest.jsonl").open("a") as handle:
        handle.write('{"partition": "torn')
    _owe(root, _chains_rel(WEDNESDAY))
    _owe(root, _chains_rel(THURSDAY))


def test_entries_hidden_behind_a_tear_refuse_rather_than_read_as_not_owed(lake_root):
    """``read_ledger`` stops at the tear, so the two files behind it would read as no debt."""
    client = _seeded_lake(lake_root)
    _torn_with_entries_behind(lake_root)

    with pytest.raises(TornLedger):
        capture_owed(lake_root, TARGET, client)


def test_a_byte_that_is_not_utf8_refuses_rather_than_reading_as_not_owed(lake_root):
    client = _seeded_lake(lake_root)
    _owe(lake_root, _chains_rel(WEDNESDAY))
    path = lake_root / "manifest.jsonl"
    path.write_bytes(path.read_bytes().replace(b"date=2026-09-16", b"date=2026-09-\xff6"))

    with pytest.raises(LedgerNotUtf8):
        capture_owed(lake_root, TARGET, client)


@pytest.mark.parametrize(
    "tail",
    ['{"partition": "torn', '{"partition": "torn{"partition": "fused"}\n'],
    ids=["torn tail", "fused last line"],
)
def test_a_damaged_last_line_with_nothing_behind_it_still_answers(lake_root, tail):
    """The bytes a read racing a write can see, so it must not page as damage."""
    client = _seeded_lake(lake_root)
    _owe(lake_root, _chains_rel(WEDNESDAY))
    with (lake_root / "manifest.jsonl").open("a") as handle:
        handle.write(tail)

    assert capture_owed(lake_root, TARGET, client).files == ((WEDNESDAY, _chains_rel(WEDNESDAY)),)


@pytest.mark.parametrize(
    "first_line",
    [b'{"partition": "torn', b"\xef\xbb\xbf" + b'{"partition": "x"}\n'],
    ids=["torn first line", "byte-order mark"],
)
def test_a_manifest_with_no_whole_entry_is_unknown(lake_root, first_line):
    (lake_root / "manifest.jsonl").write_bytes(first_line)

    assert capture_owed(lake_root, TARGET, FakeS3()).unknown == (
        "the lake's manifest holds no whole entry"
    )


def test_a_hidden_entry_pages_through_the_sweeps_containment(lake_root):
    client = _seeded_lake(lake_root)
    _torn_with_entries_behind(lake_root)

    page_list, lines = _checked(
        lake_root, client, _evening(NEXT_MONDAY), _calendar(), BUCKET_UNCHECKED_EVENT
    )

    assert len(page_list) == 1
    assert "TornLedger" in page_list[0].body
    assert lines == ("bucket copy not checked: TornLedger",)


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


def test_a_caught_raise_leaves_its_stack_trace_in_the_jobs_journal(lake_root, capsys):
    """The page carries the type alone, so stderr is the only place the reason survives."""

    def refused():
        raise bucket.BucketSettingsInvalid("bucket_region is missing, set it in config.yaml")

    lines = ReportLines()
    sweep._check_bucket_copy(
        lake_root,
        bucket_source=refused,
        now=EVENING,
        calendar=_calendar(),
        publisher=None,
        report=lines,
    )

    assert lines.lines == ("bucket copy not checked: BucketSettingsInvalid",)
    err = capsys.readouterr().err
    assert "Traceback" in err
    assert "in refused" in err
    assert "bucket_region is missing" in err


@pytest.mark.parametrize("late", [True, False], ids=["behind page", "not-checked page"])
@pytest.mark.parametrize("secret", [False, True], ids=["no transport", "carries a secret"])
def test_a_page_that_did_not_go_says_so_on_stderr_under_its_own_name(
    lake_root, capsys, late, secret
):
    client = _seeded_lake(lake_root)
    if late:
        _owe(lake_root, _chains_rel(EARLIER, "QQQ"))
    else:
        client.fail_with = client_error("AccessDenied", "HeadObject", 403)
    publisher = Publisher(
        lake_root=lake_root,
        transport=FakeTransport() if secret else None,
        secrets=("Capture",) if secret else (),
    )

    sweep._check_bucket_copy(
        lake_root,
        bucket_source=_source(client),
        now=_evening(NEXT_MONDAY),
        calendar=_calendar(),
        publisher=publisher,
        report=ReportLines(),
    )

    wanted = "bucket-check page refused" if secret else "bucket-check page not sent"
    assert f"sweep: {wanted}" in capsys.readouterr().err


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


def test_a_short_runway_still_leads_the_report(fixture_lake: FixtureLake, monkeypatch):
    """The digest is cut at its byte cap, so an ``ACTION`` line stays above this healthy one."""
    root = _lake(fixture_lake)
    client = _seed(root)
    _stub_free(monkeypatch, 0)

    outcome, _, _ = _run(root, now=EVENING, bucket_source=_source(client))

    assert outcome.nightly.report[0].startswith("disk runway")
    assert "bucket copy current" in outcome.nightly.report


@pytest.mark.parametrize("late", [True, False], ids=["late file", "no answer"])
def test_a_run_with_no_publisher_files_the_line_and_sends_nothing(lake_root, late):
    """``sweep`` allows ``None`` for a test run, so the check must not reach through it."""
    client = _seeded_lake(lake_root)
    if late:
        _owe(lake_root, _chains_rel(EARLIER, "QQQ"))
    else:
        client.fail_with = client_error("AccessDenied", "HeadObject", 403)
    lines = ReportLines()

    sweep._check_bucket_copy(
        lake_root,
        bucket_source=_source(client),
        now=_evening(NEXT_MONDAY),
        calendar=_calendar(),
        publisher=None,
        report=lines,
    )

    assert lines.kinds == (ACTION,)


def test_the_pages_match_the_designs_message_table_literally():
    # Compared against literals, because a constant compared to itself passes whatever it says.
    assert BUCKET_BEHIND_EVENT == "sweep_bucket_behind"
    assert BUCKET_BEHIND_TITLE == "Capture at risk: captured data not in the bucket"
    assert BUCKET_UNCHECKED_EVENT == "sweep_bucket_unchecked"
    assert BUCKET_UNCHECKED_TITLE == "Capture at risk: bucket copy not checked"
    design = (Path(__file__).parents[2] / "docs" / "design.md").read_text()
    assert f"`{BUCKET_BEHIND_TITLE}`" in design
    assert f"`{BUCKET_UNCHECKED_TITLE}`" in design


def test_the_pages_live_copy_figure_is_the_live_rules_two_lives():
    """The number the phone shows, read from the rule that sets it."""
    rules = (Path(__file__).parents[2] / "infra" / "live" / "bucket.tf").read_text()
    live = rules[rules.index('prefix = "lake-live/"') :]
    expires = int(re.search(r"expiration \{\s*days\s*=\s*(\d+)", live).group(1))
    noncurrent = int(re.search(r"noncurrent_days\s*=\s*(\d+)", live).group(1))

    assert expires + noncurrent == sweep.LIVE_COPY_DAYS


def test_the_body_tells_chains_from_quotes(fixture_lake: FixtureLake):
    """Chains have no live copy, so the 97 days must never read as their margin."""
    root = _lake(fixture_lake)
    client = _seed(root)
    _owe(root, _chains_rel(EARLIER, "QQQ"))

    _, _, transport = _run(root, now=EVENING, bucket_source=_source(client))

    (page,) = _pages(transport)
    assert "Chains have no other copy" in page.body
    assert f"quotes' live copies expire about {sweep.LIVE_COPY_DAYS} days" in page.body


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


def test_a_shadow_hosts_evening_run_gets_no_source(tmp_path, lake_root, monkeypatch):
    path = _bucket_config(tmp_path, lake_root, role="shadow")
    seen: dict[str, object] = {}
    monkeypatch.setattr(sweep, "sweep", lambda **kwargs: seen.update(kwargs))
    monkeypatch.setattr(sweep, "load_tickers", lambda _path: None)

    sweep.sweep_from_config(config_path=path)

    assert seen["bucket_source"] is None


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
    assert f"sweep: bucket copy behind: 1 captured file, oldest {EARLIER}" in (
        capsys.readouterr().out
    )


def test_the_hand_check_measures_from_the_clock_it_is_given(
    tmp_path, lake_root, monkeypatch, capsys
):
    """Tonight's file is not late, and the real clock would call it late."""
    client = _seeded_lake(lake_root)
    _owe(lake_root, _chains_rel(FOLLOWING, "QQQ"))
    path = _bucket_config(tmp_path, lake_root)
    transport = FakeTransport()
    monkeypatch.setattr(bucket, "connect", lambda config: (TARGET, client))
    monkeypatch.setattr("lake.alert.NtfyTransport", lambda topic: transport)
    monkeypatch.setattr(sweep, "ExchangeCalendar", _calendar)

    code = sweep.main(
        ["--config", str(path), "--bucket-check"],
        clock=ManualClock(_evening(FOLLOWING)),
    )

    # Owed and not late sends no page, and it is not the all-clear either.
    assert code == 1
    assert transport.messages == []
    assert capsys.readouterr().out == f"sweep: bucket copy owes 1 from {FOLLOWING}\n"


def test_the_hand_check_exits_one_when_the_bucket_will_not_answer(
    tmp_path, lake_root, monkeypatch, capsys
):
    """The runbook takes exit 0 as a repair confirmed, so a failed read must not exit 0."""
    client = _seeded_lake(lake_root)
    client.fail_with = client_error("AccessDenied", "HeadObject", 403)
    path = _bucket_config(tmp_path, lake_root)
    transport = FakeTransport()
    monkeypatch.setattr(bucket, "connect", lambda config: (TARGET, client))
    monkeypatch.setattr("lake.alert.NtfyTransport", lambda topic: transport)

    code = sweep.main(
        ["--config", str(path), "--bucket-check"],
        clock=ManualClock(_evening(NEXT_MONDAY)),
    )

    assert code == 1
    assert len(_pages(transport, BUCKET_UNCHECKED_EVENT)) == 1
    assert "sweep: bucket copy not checked: bucket refused" in capsys.readouterr().out


def test_the_hand_check_answers_a_bad_config_with_one_line_and_exit_two(tmp_path, capsys):
    path = tmp_path / "config.yaml"
    path.write_text("lake_root: [not, a, path\n")

    with pytest.raises(SystemExit) as exited:
        sweep.main(["--config", str(path), "--bucket-check"])

    assert exited.value.code == 2


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
    assert capsys.readouterr().out == "sweep: bucket copy current\n"


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
