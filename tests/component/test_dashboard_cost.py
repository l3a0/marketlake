"""The dashboard's Cost panel query, ``/api/cost``, marketlake #913.

The query reads the sweep's cost files under ``reports/aws_cost/date=D/`` and makes every
decision the panel shows, because nothing in the repository runs the page's JavaScript.
Each file here is literal JSON in the shape ``lake.aws_cost`` writes, so a change to the
writer's shape that the query does not follow fails here rather than on the page.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from lake import dashboard
from lake.dashboard import DashboardService, billed_size, classify_s3_usage, is_timed_storage
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock

NOW = datetime(2026, 10, 19, 14, 0, tzinfo=UTC)


def service(root: Path) -> DashboardService:
    return DashboardService(
        root,
        clock=ManualClock(NOW),
        calendar=weekday_sessions(date(2026, 10, 12), date(2026, 10, 26)),
        page=b"<!doctype html>",
        icon=b"",
    )


def cost(root: Path) -> dict:
    return service(root).run_query("cost", {})


def file(root: Path, night: str, entry: dict | str, *, stamp: str = "183000000000") -> Path:
    directory = root / "reports" / "aws_cost" / f"date={night}"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{stamp}-4242.json"
    path.write_text(entry if isinstance(entry, str) else json.dumps(entry))
    return path


def entry(night: str, period: str, state: str, **extra) -> dict:
    return {
        "at": f"{night}T18:30:00-04:00",
        "day": night,
        "period": period,
        "state": state,
        "error": None,
        "currency": "USD",
        **extra,
    }


def october(**changes) -> dict:
    """A current block for October through the 15th, the shape the writer gives it."""
    block = {
        "period": "2026-10",
        "delivered_at": "2026-10-16T06:01:00+00:00",
        "days_in_month": 31,
        "newest_day": "2026-10-15",
        "complete_through": "2026-10-14",
        "total": 13.65,
        "mtd": {
            "service": {
                "AmazonEC2": {"usage": 10.15, "other": 3.5},
                "AmazonS3": {"usage": 0.4, "other": 0.0},
            },
            "usage_type": {},
            "resource": {},
            "s3": {},
        },
        "daily": {"2026-10-13": {"AmazonEC2": 0.7}, "2026-10-14": {"AmazonEC2": 0.7}},
        "storage_daily": {},
        "sessions": 10,
        "cost_per_session": {
            "status": "ok",
            "sessions": 10,
            "usage_cost": 9.8,
            "value": 0.98,
            "through": "2026-10-14",
        },
        "projection": {
            "status": "estimate",
            "month_end": 25.2,
            "usage_through": 9.8,
            "other": 3.5,
            "window_days": 14,
            "through": "2026-10-14",
        },
    }
    block.update(changes)
    return block


def september(**changes) -> dict:
    block = {
        "period": "2026-09",
        "delivered_at": "2026-10-02T06:01:00+00:00",
        "days_in_month": 30,
        "total": 18.25,
        "daily": {"2026-09-30": {"AmazonEC2": 0.6}, "2026-09-20": {"AmazonEC2": 0.6}},
        "storage_daily": {},
        "sessions": 20,
        "cost_per_session": {
            "status": "ok",
            "sessions": 20,
            "usage_cost": 18.0,
            "value": 0.9,
            "through": "2026-09-30",
        },
    }
    block.update(changes)
    return block


@pytest.fixture
def root(tmp_path: Path) -> Path:
    lake = tmp_path / "lake"
    lake.mkdir()
    return lake


# -- the conditions ------------------------------------------------------------------------


def test_a_host_with_no_cost_files_says_it_runs_no_read(root: Path):
    payload = cost(root)

    assert payload["condition"] == "no_directory"
    assert payload["state"] is None and payload["current"] is None


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a directory whatever its mode")
def test_an_unreadable_directory_is_named_not_read_as_empty(root: Path):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "no_export"))
    directory = root / "reports" / "aws_cost"
    directory.chmod(0)
    try:
        payload = cost(root)
    finally:
        directory.chmod(0o755)

    assert payload["condition"] == "unreadable_directory"
    assert payload["error"] == "PermissionError"


def test_an_oversize_file_is_counted_and_an_unparseable_one_skipped(root: Path, monkeypatch):
    monkeypatch.setattr(dashboard, "HISTORY_REPORT_MAX_BYTES", 400)
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "no_export"), stamp="183000000000")
    file(root, "2026-10-16", "{not json", stamp="190000000000")
    file(root, "2026-10-16", "[1, 2]", stamp="191000000000")
    file(root, "2026-10-16", json.dumps({"padding": "x" * 500}), stamp="192000000000")

    payload = cost(root)

    assert payload["condition"] is None
    assert payload["oversize"] == 1
    assert payload["unparseable"] == 2
    assert payload["state"] == "no_export"
    assert payload["files_read"] == 1


def test_no_readable_file_is_its_own_condition(root: Path):
    file(root, "2026-10-16", "{not json")

    payload = cost(root)

    assert payload["condition"] == "no_file"
    assert payload["unparseable"] == 1


@pytest.mark.parametrize("state", ["no_export", "no_delivery", "refreshing", "failed"])
def test_each_state_the_newest_file_names_is_returned(root: Path, state: str):
    error = "ClientError" if state == "failed" else None
    file(root, "2026-10-16", {**entry("2026-10-16", "2026-10", state), "error": error})

    payload = cost(root)

    assert payload["state"] == state
    assert payload["newest"] == {
        "day": "2026-10-16",
        "at": "2026-10-16T18:30:00-04:00",
        "error": error,
    }
    assert payload["no_current_delivery"] is True
    assert payload["current"] is None


def test_a_state_the_query_does_not_know_is_unknown(root: Path):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "rebooted"))

    assert cost(root)["state"] == "unknown"


# -- which file answers what ---------------------------------------------------------------


def test_a_failed_newest_file_keeps_the_last_good_figures_of_its_period(root: Path):
    file(root, "2026-10-15", entry("2026-10-15", "2026-10", "delivery", current=october()))
    file(root, "2026-10-16", {**entry("2026-10-16", "2026-10", "failed"), "error": "ClientError"})

    payload = cost(root)

    assert payload["state"] == "failed"
    assert payload["newest"]["error"] == "ClientError"
    current = payload["current"]
    assert current["from_day"] == "2026-10-15"
    assert current["total"] == 13.65
    assert current["services"] == [
        {"product": "AmazonEC2", "usage": 10.15, "other": 3.5, "total": 13.65},
        {"product": "AmazonS3", "usage": 0.4, "other": 0.0, "total": 0.4},
    ]
    assert current["projection"]["month_end"] == 25.2
    assert current["cost_per_session"]["value"] == 0.98


def test_on_the_first_last_month_never_shows_as_month_to_date(root: Path):
    """The newest file is the 1st's ``no_delivery`` for November, which carries October as its
    previous period. The newest ``delivery`` file is the 31st's, and its period is October,
    so it is not November's month to date."""
    file(root, "2026-10-31", entry("2026-10-31", "2026-10", "delivery", current=october()))
    file(
        root,
        "2026-11-02",
        entry(
            "2026-11-02",
            "2026-11",
            "no_delivery",
            previous=september(
                period="2026-10", total=21.1, daily={"2026-10-31": {"AmazonEC2": 0.7}}
            ),
        ),
    )

    payload = cost(root)

    assert payload["state"] == "no_delivery"
    assert payload["period"] == "2026-11"
    assert payload["nights_scanned"] == 2
    assert payload["no_current_delivery"] is True
    assert payload["current"] is None
    assert payload["previous"]["period"] == "2026-10"
    assert payload["previous"]["total"] == 21.1
    assert payload["trend"][-1] == {
        "day": "2026-10-31",
        "total": 0.7,
        "by_service": {"AmazonEC2": 0.7},
    }
    assert payload["s3"] == []


def test_a_previous_block_for_the_wrong_period_is_not_taken(root: Path):
    file(
        root,
        "2026-10-14",
        entry("2026-10-14", "2026-10", "delivery", current=october(), previous=september()),
    )
    file(
        root,
        "2026-10-16",
        entry(
            "2026-10-16",
            "2026-10",
            "delivery",
            current=october(),
            previous=september(period="2026-08"),
        ),
    )

    payload = cost(root)

    assert payload["current"]["from_day"] == "2026-10-16"
    assert payload["previous"]["from_day"] == "2026-10-14"
    assert payload["previous"]["cost_per_session"]["value"] == 0.9


def test_the_newest_file_is_stale_when_a_newer_nightly_report_exists(root: Path):
    file(root, "2026-10-15", entry("2026-10-15", "2026-10", "delivery", current=october()))
    (root / "reports" / "2026-10-16-183000000000-4242.json").write_text("{}")

    payload = cost(root)

    assert payload["stale"] is True
    assert payload["newest_nightly"] == "2026-10-16"


def test_the_newest_file_is_not_stale_beside_its_own_night(root: Path):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=october()))
    (root / "reports" / "2026-10-16-183000000000-4242.json").write_text("{}")

    assert cost(root)["stale"] is False


def test_the_scan_stops_a_day_before_the_current_period_starts(root: Path):
    """The newest file names October, so the scan reads back to 30 September, which can hold
    an October file filed late on the Eastern night, and opens nothing older."""
    file(root, "2026-10-16", {**entry("2026-10-16", "2026-10", "failed"), "error": "X"})
    file(root, "2026-09-30", entry("2026-09-30", "2026-10", "delivery", current=october()))
    for day in range(1, 30):
        file(root, f"2026-09-{day:02d}", "{not json")

    payload = cost(root)

    assert payload["nights_scanned"] == 2
    assert payload["unparseable"] == 0
    assert payload["current"]["from_day"] == "2026-09-30"


def test_nothing_in_a_file_of_the_wrong_types_raises(root: Path):
    file(
        root,
        "2026-10-16",
        {
            "state": "delivery",
            "period": "2026-10",
            "day": 16,
            "at": ["x"],
            "currency": 1,
            "current": {
                "period": "2026-10",
                "total": "lots",
                "newest_day": "not a day",
                "complete_through": 7,
                "mtd": {"service": {"AmazonEC2": "x"}, "s3": {"b": {"Requests-Tier1": True}}},
                "daily": {"2026-10-01": "x", "garbage": {}},
                "storage_daily": {"b": {"TimedStorage-ByteHrs": {"2026-10-01": "x"}}},
                "projection": "x",
                "cost_per_session": {"status": "ok", "value": "x"},
            },
            "previous": [],
        },
    )

    payload = cost(root)

    assert payload["state"] == "delivery"
    current = payload["current"]
    assert current["total"] is None
    assert current["services"] == [
        {"product": "AmazonEC2", "usage": 0.0, "other": 0.0, "total": 0.0}
    ]
    assert current["projection"] is None
    assert current["cost_per_session"] == {"status": "ok", "value": None}
    assert payload["previous"] is None
    assert payload["s3"] == [
        {
            "bucket": "b",
            "split": [],
            "groups": {},
            "size_day": None,
            "size_gb": None,
            "classes": [],
            "growth": None,
        }
    ]


# -- the trend -----------------------------------------------------------------------------


def test_the_trend_is_thirty_calendar_days_from_both_series(root: Path):
    previous = september(daily={"2026-09-16": {"AmazonEC2": 0.5}, "2026-09-15": {"AmazonEC2": 9.9}})
    current = october(
        daily={"2026-10-01": {"AmazonEC2": 0.6, "AmazonS3": 0.1}, "2026-10-15": {"AmazonEC2": 0.3}}
    )
    file(
        root,
        "2026-10-16",
        entry("2026-10-16", "2026-10", "delivery", current=current, previous=previous),
    )

    trend = cost(root)["trend"]

    assert len(trend) == 30
    assert trend[0] == {"day": "2026-09-16", "total": 0.5, "by_service": {"AmazonEC2": 0.5}}
    assert trend[-1]["day"] == "2026-10-15" and trend[-1]["total"] == 0.3
    by_day = {row["day"]: row for row in trend}
    assert by_day["2026-10-01"]["total"] == 0.7
    assert by_day["2026-10-01"]["by_service"] == {"AmazonEC2": 0.6, "AmazonS3": 0.1}
    # A day neither series carries has no figure rather than a zero.
    assert by_day["2026-10-02"] == {"day": "2026-10-02", "total": None, "by_service": {}}
    assert "2026-09-15" not in by_day


# -- the S3 split and the billed size ------------------------------------------------------


@pytest.mark.parametrize(
    ("usage_type", "expected"),
    [
        ("TimedStorage-ByteHrs", ("storage", "Standard")),
        ("TimedStorage-SIA-ByteHrs", ("storage", "Standard-IA")),
        ("USE2-TimedStorage-SIA-ByteHrs", ("storage", "Standard-IA")),
        ("TimedStorage-GDA-ByteHrs", ("storage", "Glacier Deep Archive")),
        ("TimedStorage-NewClass-ByteHrs", ("storage", "TimedStorage-NewClass-ByteHrs")),
        ("Requests-Tier1", ("requests", "PUT, COPY, POST and LIST requests")),
        ("Requests-Tier2", ("requests", "GET and other requests")),
        ("Requests-SIA-Tier1", ("requests", "Requests-SIA-Tier1")),
        ("DataTransfer-Out-Bytes", ("transfer", "DataTransfer-Out-Bytes")),
        ("EarlyDelete-SIA", ("early delete", "EarlyDelete-SIA")),
        ("Retrieval-SIA", ("retrieval", "Retrieval-SIA")),
        ("StorageAnalytics-ObjCount", ("other", "StorageAnalytics-ObjCount")),
    ],
)
def test_classify_s3_usage(usage_type: str, expected: tuple[str, str]):
    assert classify_s3_usage(usage_type) == expected


def test_only_timed_storage_lines_make_a_size():
    assert is_timed_storage("TimedStorage-ByteHrs")
    assert is_timed_storage("USE2-TimedStorage-SIA-ByteHrs")
    assert not is_timed_storage("EarlyDelete-SIA")
    assert not is_timed_storage("Requests-Tier1")
    assert not is_timed_storage("XTimedStorage-ByteHrs")


def test_billed_size_is_one_days_amount_times_the_months_days():
    # 50 GB held all of 14 October is 50 / 31 GB-months that day.
    assert billed_size(50 / 31, date(2026, 10, 14)) == pytest.approx(50.0)
    # The same amount in a 30-day month is a smaller size.
    assert billed_size(50 / 31, date(2026, 9, 14)) == pytest.approx(50 * 30 / 31)


def test_each_bucket_shows_its_split_beside_its_billed_size_and_growth(root: Path):
    """Pass 7's bucket: growing 0.57 GB a session, 110 GB on 1 October with 10 GB of it in
    Standard-IA, 115.7 GB on the 14th after ten sessions. The size is taken on N - 1."""
    current = october(
        mtd={
            "service": {},
            "s3": {
                "example-lake": {
                    "TimedStorage-ByteHrs": 2.4,
                    "TimedStorage-SIA-ByteHrs": 0.12,
                    "Requests-Tier1": 0.05,
                    "Requests-Tier2": 0.01,
                    "DataTransfer-Out-Bytes": 0.02,
                    "EarlyDelete-SIA": 0.03,
                    "Mystery-Type": 0.001,
                }
            },
        },
        storage_daily={
            "example-lake": {
                "TimedStorage-ByteHrs": {
                    "2026-10-01": 100 / 31,
                    "2026-10-14": 105.7 / 31,
                    "2026-10-15": 106 / 31,
                },
                "TimedStorage-SIA-ByteHrs": {"2026-10-01": 10 / 31, "2026-10-14": 10 / 31},
                "EarlyDelete-SIA": {"2026-10-14": 999.0},
            }
        },
    )
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=current))

    [bucket] = cost(root)["s3"]

    assert bucket["bucket"] == "example-lake"
    assert bucket["size_day"] == "2026-10-14"
    assert bucket["size_gb"] == pytest.approx(115.7)
    assert bucket["classes"] == [
        {"usage_type": "TimedStorage-ByteHrs", "label": "Standard", "gb": pytest.approx(105.7)},
        {"usage_type": "TimedStorage-SIA-ByteHrs", "label": "Standard-IA", "gb": pytest.approx(10)},
    ]
    assert bucket["growth"] == {
        "from": "2026-10-01",
        "days": 13,
        "gb": pytest.approx(5.7),
        "gb_per_day": pytest.approx(5.7 / 13, abs=1e-6),
    }
    assert bucket["groups"] == {
        "early delete": 0.03,
        "other": 0.001,
        "requests": 0.06,
        "storage": 2.52,
        "transfer": 0.02,
    }
    assert [row["usage_type"] for row in bucket["split"]] == [
        "TimedStorage-ByteHrs",
        "TimedStorage-SIA-ByteHrs",
        "Requests-Tier1",
        "EarlyDelete-SIA",
        "DataTransfer-Out-Bytes",
        "Requests-Tier2",
        "Mystery-Type",
    ]
    assert bucket["split"][-1] == {
        "usage_type": "Mystery-Type",
        "group": "other",
        "label": "Mystery-Type",
        "cost": 0.001,
    }


def test_growth_reaches_into_the_closed_month_inside_the_trend_window(root: Path):
    previous = september(
        storage_daily={
            "example-lake": {"TimedStorage-ByteHrs": {"2026-09-10": 1.0, "2026-09-20": 90 / 30}}
        }
    )
    current = october(
        storage_daily={"example-lake": {"TimedStorage-ByteHrs": {"2026-10-14": 93.1 / 31}}}
    )
    file(
        root,
        "2026-10-16",
        entry("2026-10-16", "2026-10", "delivery", current=current, previous=previous),
    )

    [bucket] = cost(root)["s3"]

    # 2026-09-10 is before the trend's first day, 16 September, so the growth starts on the
    # 20th, whose size is read with September's 30 days.
    assert bucket["growth"]["from"] == "2026-09-20"
    assert bucket["growth"]["gb"] == pytest.approx(3.1)
    assert bucket["growth"]["days"] == 24


def test_a_size_with_no_storage_line_on_the_newest_complete_day_is_not_yet_available(
    root: Path,
):
    current = october(
        mtd={"service": {}, "s3": {"example-lake": {"Requests-Tier1": 0.05}}},
        storage_daily={"example-lake": {"TimedStorage-ByteHrs": {"2026-10-12": 3.0}}},
    )
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=current))

    [bucket] = cost(root)["s3"]

    assert bucket["size_gb"] is None and bucket["growth"] is None and bucket["classes"] == []
