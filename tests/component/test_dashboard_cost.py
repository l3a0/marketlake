"""The dashboard's Cost panel query, ``/api/cost``, marketlake #913.

The query reads the sweep's cost files under ``reports/aws_cost/date=D/`` and makes every
decision the panel shows, so the page computes only where to draw.
``tests/component/test_status_page_cost.py`` runs the page against what this returns.
Each file here is literal JSON in the shape ``lake.aws_cost`` writes, so a change to the
writer's shape that the query does not follow fails here rather than on the page.

``october`` and ``september`` are the shape written before #921, with no daily usage split,
so every test that does not name the split also shows an old report still reads.
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


def service(root: Path, *, configured: bool | None = None) -> DashboardService:
    return DashboardService(
        root,
        clock=ManualClock(NOW),
        calendar=weekday_sessions(date(2026, 10, 12), date(2026, 10, 26)),
        cost_export_configured=configured,
        page=b"<!doctype html>",
        icon=b"",
    )


def cost(root: Path, *, configured: bool | None = None) -> dict:
    return service(root, configured=configured).run_query("cost", {})


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


def unsplit(day: str, total: float | None, by_service: dict, *, partial: bool = False) -> dict:
    """A trend row from a block written before #921, which carries no usage split."""
    return {
        "day": day,
        "partial": partial,
        "total": total,
        "by_service": by_service,
        "usage": None,
        "usage_by_series": None,
        "usage_by_service": None,
        "credits": None,
        "other_lines": None,
        "other_by_type": None,
    }


@pytest.fixture
def root(tmp_path: Path) -> Path:
    lake = tmp_path / "lake"
    lake.mkdir()
    return lake


# -- the conditions ------------------------------------------------------------------------


def test_a_host_with_no_cost_files_has_no_directory(root: Path):
    payload = cost(root)

    assert payload["condition"] == "no_directory"
    assert payload["state"] is None and payload["current"] is None


# The config, not the directory, says why ``no_directory`` holds, marketlake #911. On
# 2026-10-16 the VM's config named an export, no sweep had run since the deploy, and the
# panel blamed config.yaml. The flag is asserted with ``is``, so ``False`` and ``None``
# cannot pass for each other. The sentences are literals here rather than built from
# ``VENDOR_SWEEP``, so a change to the schedule shows up as a changed sentence.


def test_no_directory_beside_a_configured_export_says_it_is_configured(root: Path):
    payload = cost(root, configured=True)

    assert payload["condition"] == "no_directory"
    assert payload["cost_export_configured"] is True
    assert payload["no_report_text"] == (
        "No cost report has been filed. This host's config.yaml named a cost_export when the "
        "dashboard started, and no sweep has filed a report yet. The cost read runs in the "
        "weekday 18:30 sweep, whose nightly report names any failure."
    )


def test_no_directory_beside_a_config_with_no_export_says_it_names_none(root: Path):
    payload = cost(root, configured=False)

    assert payload["condition"] == "no_directory"
    assert payload["cost_export_configured"] is False
    assert payload["no_report_text"] == (
        "No cost report has been filed. This host's config.yaml named no cost_export when the "
        "dashboard started, and a host without one runs no cost read."
    )


def test_no_directory_on_a_service_that_read_no_config_claims_no_cause(root: Path):
    """``--lake-root`` serves a lake directly and never reads config.yaml."""
    payload = cost(root)

    assert payload["condition"] == "no_directory"
    assert payload["cost_export_configured"] is None
    assert payload["no_report_text"] == "No cost report has been filed."


@pytest.mark.parametrize("configured", [True, False])
def test_the_configured_fact_rides_on_an_empty_directory(root: Path, configured: bool):
    (root / "reports" / "aws_cost").mkdir(parents=True)

    payload = cost(root, configured=configured)

    assert payload["condition"] == "no_file"
    assert payload["cost_export_configured"] is configured
    assert payload["no_report_text"] is None


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a directory whatever its mode")
@pytest.mark.parametrize("configured", [True, False])
def test_the_configured_fact_rides_on_an_unreadable_directory(root: Path, configured: bool):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "no_export"))
    directory = root / "reports" / "aws_cost"
    directory.chmod(0)
    try:
        payload = cost(root, configured=configured)
    finally:
        directory.chmod(0o755)

    assert payload["condition"] == "unreadable_directory"
    assert payload["cost_export_configured"] is configured
    assert payload["no_report_text"] is None


@pytest.mark.parametrize("configured", [True, False, None])
def test_the_configured_fact_rides_on_a_payload_that_read_a_file(
    root: Path, configured: bool | None
):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "no_export"))

    payload = cost(root, configured=configured)

    assert payload["condition"] is None
    assert payload["cost_export_configured"] is configured
    assert payload["no_report_text"] is None


@pytest.mark.parametrize("configured", [True, False, None])
def test_the_configured_fact_rides_on_the_backstop_too(root: Path, configured: bool | None):
    file(root, "2026-10-16", entry("2026-10-16", "0001-01", "failed"))

    payload = cost(root, configured=configured)

    assert payload["condition"] == "failed"
    assert payload["cost_export_configured"] is configured


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
        "previous_error": None,
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
    # The closed month's last day is complete, so nothing in the trend is partial.
    assert payload["trend"][-1] == unsplit("2026-10-31", 0.7, {"AmazonEC2": 0.7})
    assert not any(row["partial"] for row in payload["trend"])
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
    # The read can run and its file fail to write, so the sentence says what the files show.
    assert payload["stale_text"] == (
        "The newest cost report is from 2026-10-15, older than the newest nightly report, "
        "from 2026-10-16. No readable cost report was filed that night."
    )


def test_the_newest_file_is_not_stale_beside_its_own_night(root: Path):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=october()))
    (root / "reports" / "2026-10-16-183000000000-4242.json").write_text("{}")

    payload = cost(root)

    assert payload["stale"] is False
    assert payload["stale_text"] is None


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


def test_the_later_file_of_one_night_is_the_newest(root: Path):
    """A second sweep on one night writes a second file, and its stamp sorts later. The
    later file is the night's answer, so a retry that worked replaces a failure."""
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "failed"), stamp="183000000000")
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "no_export"), stamp="210000000000")

    assert cost(root)["state"] == "no_export"


def test_an_unreadable_newest_night_does_not_hide_the_night_before(root: Path):
    """The scan's first bound is the day before the newest night's month starts, so a night
    whose only file is unreadable still lets the night before it answer."""
    file(root, "2026-10-16", "{not json")
    file(root, "2026-10-15", entry("2026-10-15", "2026-10", "delivery", current=october()))

    payload = cost(root)

    assert payload["condition"] is None
    assert payload["unparseable"] == 1
    assert payload["state"] == "delivery"
    assert payload["current"]["from_day"] == "2026-10-15"


def test_no_readable_file_still_bounds_the_scan(root: Path):
    """With no readable file, the bound from the newest night's month is all that stops the
    scan, and an August night is outside it."""
    file(root, "2026-10-16", "{not json")
    file(root, "2026-08-03", "{not json")

    payload = cost(root)

    assert payload["condition"] == "no_file"
    assert payload["nights_scanned"] == 1
    assert payload["unparseable"] == 1


def test_a_late_file_bounds_the_scan_by_its_own_period(root: Path):
    """A file written late on 31 October Eastern names November. Its period moves the bound
    to 31 October, so the scan opens no October night before it."""
    file(root, "2026-10-31", entry("2026-10-31", "2026-11", "no_delivery"))
    file(root, "2026-10-20", "{not json")

    payload = cost(root)

    assert payload["period"] == "2026-11"
    assert payload["nights_scanned"] == 1
    assert payload["unparseable"] == 0


def test_the_currency_comes_from_the_file_with_the_figures(root: Path):
    """A failed read carries no currency. The figures shown are the earlier delivery's, so
    the currency is that file's too."""
    file(root, "2026-10-15", entry("2026-10-15", "2026-10", "delivery", current=october()))
    file(root, "2026-10-16", {**entry("2026-10-16", "2026-10", "failed"), "currency": None})

    payload = cost(root)

    assert payload["state"] == "failed"
    assert payload["currency"] == "USD"


def test_stale_compares_against_the_newest_nightly_report(root: Path):
    """An older nightly report beside a newer one must not make a cost file look current."""
    file(root, "2026-10-15", entry("2026-10-15", "2026-10", "delivery", current=october()))
    (root / "reports" / "2026-10-14-183000000000-4242.json").write_text("{}")
    (root / "reports" / "2026-10-16-183000000000-4242.json").write_text("{}")

    payload = cost(root)

    assert payload["newest_nightly"] == "2026-10-16"
    assert payload["stale"] is True


def test_an_impossible_period_reaches_the_backstop_and_does_not_raise(root: Path):
    """A period of ``0001-01`` has no month before it, so the scan raises ``OverflowError``.
    The query's catch turns that into a ``failed`` condition rather than a 500."""
    file(root, "2026-10-16", entry("2026-10-16", "0001-01", "failed"))

    payload = cost(root)

    assert payload["condition"] == "failed"
    assert payload["error"] == "OverflowError"


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
            "size_series": [],
        }
    ]


# -- the trend -----------------------------------------------------------------------------


@pytest.mark.parametrize("bare", ["NaN", "Infinity", "-Infinity"])
def test_a_number_that_is_not_finite_never_reaches_the_page(root: Path, bare: str):
    """Python's ``json`` reads a bare ``NaN`` and would write it back out, and the page's
    ``JSON.parse`` refuses it, which would leave the whole Cost panel unreadable."""
    current = october(total="__total__")
    current["projection"]["month_end"] = "__month_end__"
    text = json.dumps(entry("2026-10-16", "2026-10", "delivery", current=current))
    text = text.replace('"__total__"', bare).replace('"__month_end__"', bare)
    file(root, "2026-10-16", text)

    payload = cost(root)

    assert payload["current"]["total"] is None
    assert payload["current"]["projection"]["month_end"] is None
    json.dumps(payload, allow_nan=False)


def test_a_previous_error_is_named_rather_than_read_as_a_first_month(root: Path):
    """The writer names a failed read of the closed month in ``previous_error``, beside a
    current block that still reads. The panel names that failure, because the first-month
    explanation would state a cause nothing checked."""
    file(
        root,
        "2026-10-16",
        entry(
            "2026-10-16",
            "2026-10",
            "delivery",
            current=october(),
            previous_error="a data file's Parquet footer could not be read",
        ),
    )

    payload = cost(root)

    assert payload["state"] == "delivery"
    assert payload["current"]["total"] == pytest.approx(13.65)
    assert payload["previous"] is None
    assert payload["newest"]["previous_error"] == "a data file's Parquet footer could not be read"
    assert payload["previous_text"] == (
        "The closed month's read failed: a data file's Parquet footer could not be read. "
        "Its figures are not available."
    )


def test_no_previous_block_and_no_error_reads_as_the_export_first_month(root: Path):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=october()))

    payload = cost(root)

    assert payload["previous"] is None
    assert payload["newest"]["previous_error"] is None
    assert payload["previous_text"] == (
        "The closed month's figures are not available yet. The export's first month has no "
        "closed month, and a closed month that AWS is still delivering is read once its "
        "delivery finishes."
    )


def test_a_previous_block_leaves_no_closed_month_sentence(root: Path):
    file(
        root,
        "2026-10-16",
        entry("2026-10-16", "2026-10", "delivery", current=october(), previous=september()),
    )

    payload = cost(root)

    assert payload["previous"]["period"] == "2026-09"
    assert payload["previous_text"] is None


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
    assert trend[0] == unsplit("2026-09-16", 0.5, {"AmazonEC2": 0.5})
    assert trend[-1]["day"] == "2026-10-15" and trend[-1]["total"] == 0.3
    by_day = {row["day"]: row for row in trend}
    assert by_day["2026-10-01"]["total"] == 0.7
    assert by_day["2026-10-01"]["by_service"] == {"AmazonEC2": 0.6, "AmazonS3": 0.1}
    # A day neither series carries has no figure rather than a zero.
    assert by_day["2026-10-02"] == unsplit("2026-10-02", None, {})
    # The newest usage day, and only it, is partial.
    assert [row["day"] for row in trend if row["partial"]] == ["2026-10-15"]
    assert "2026-09-15" not in by_day


def test_the_current_series_wins_a_day_both_carry(root: Path):
    """The current period's series is the newer reading of a day both blocks carry."""
    previous = september(daily={"2026-10-01": {"AmazonEC2": 9.9}})
    current = october(daily={"2026-10-01": {"AmazonEC2": 0.7}})
    file(
        root,
        "2026-10-16",
        entry("2026-10-16", "2026-10", "delivery", current=current, previous=previous),
    )

    by_day = {row["day"]: row for row in cost(root)["trend"]}

    assert by_day["2026-10-01"]["total"] == 0.7


# -- usage and credits apart, #921 -------------------------------------------------------


def split_october(**changes) -> dict:
    """October as the writer gives it since #921, credits offsetting every dollar of usage.

    The 14th carries $0.20 of EC2, $0.05 of S3, $0.10 of the VM's public address and $0.03
    of CloudWatch, a credit of $0.38 and $0.02 of tax: a net of $0.02. The 15th, the newest
    usage day, carries $0.10 of EC2 and a credit of $0.10.
    """
    block = october(
        daily={
            "2026-10-14": {
                "AmazonEC2": 0.2 - 0.38 + 0.02,
                "AmazonS3": 0.05,
                "AmazonVPC": 0.1,
                "AmazonCloudWatch": 0.03,
            },
            "2026-10-15": {"AmazonEC2": 0.0},
        },
        daily_usage={
            "2026-10-14": {
                "AmazonEC2": 0.2,
                "AmazonS3": 0.05,
                "AmazonVPC": 0.1,
                "AmazonCloudWatch": 0.03,
            },
            "2026-10-15": {"AmazonEC2": 0.1},
        },
        daily_other={
            "2026-10-14": {"Credit": -0.38, "Tax": 0.02},
            "2026-10-15": {"Credit": -0.1},
        },
    )
    block["mtd"] = {
        **block["mtd"],
        "line_type": {"Usage": 10.0, "DiscountedUsage": 0.55, "Credit": -10.55, "Tax": 0.1},
    }
    block.update(changes)
    return block


def test_a_trend_day_carries_its_usage_by_series_apart_from_its_credits(root: Path):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=split_october()))

    payload = cost(root)

    assert payload["trend_split"] is True
    assert payload["trend_split_text"] is None
    by_day = {row["day"]: row for row in payload["trend"]}
    day = by_day["2026-10-14"]
    assert day["total"] == pytest.approx(0.02)
    assert day["usage"] == pytest.approx(0.38)
    # CloudWatch has no slot of its own, so it folds into other services, and the raw
    # product codes stay beside the series for the tooltip and the table.
    assert day["usage_by_series"] == {
        "AmazonEC2": pytest.approx(0.2),
        "AmazonS3": pytest.approx(0.05),
        "AmazonVPC": pytest.approx(0.1),
        "other_services": pytest.approx(0.03),
    }
    assert day["usage_by_service"]["AmazonCloudWatch"] == pytest.approx(0.03)
    assert day["credits"] == pytest.approx(-0.38)
    assert day["other_lines"] == pytest.approx(0.02)
    assert day["other_by_type"] == {"Credit": pytest.approx(-0.38), "Tax": pytest.approx(0.02)}
    assert day["partial"] is False
    newest = by_day["2026-10-15"]
    assert newest["partial"] is True
    assert newest["usage"] == pytest.approx(0.1) and newest["total"] == pytest.approx(0.0)
    assert newest["other_lines"] == 0.0
    # A day the split block does not carry has no figure, split or net.
    assert by_day["2026-10-13"]["total"] is None and by_day["2026-10-13"]["usage"] is None


def test_the_series_keep_their_slots_whichever_are_present(root: Path):
    """A color follows its service, so a missing series moves no other one's slot."""
    current = split_october(
        daily={"2026-10-15": {"AmazonS3": 0.05}},
        daily_usage={"2026-10-15": {"AmazonS3": 0.05}},
        daily_other={"2026-10-15": {"Credit": -0.05}},
    )
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=current))

    series = cost(root)["trend_series"]

    assert [(row["key"], row["slot"], row["kind"]) for row in series] == [
        ("AmazonS3", 2, "usage"),
        ("credits", 6, "credit"),
    ]


def test_every_series_in_slot_order(root: Path):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=split_october()))

    series = cost(root)["trend_series"]

    assert [(row["key"], row["slot"]) for row in series] == [
        ("AmazonEC2", 1),
        ("AmazonS3", 2),
        ("AmazonVPC", 3),
        ("other_services", 4),
        ("other_lines", 5),
        ("credits", 6),
    ]
    assert all(isinstance(row["label"], str) and row["label"] for row in series)


def test_an_old_report_draws_the_net_and_says_why(root: Path):
    """A report written before #921 has no split. The trend keeps its net figures and says
    so, rather than drawing usage as zero."""
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=october()))

    payload = cost(root)

    assert payload["trend_split"] is False
    assert payload["trend_split_text"] == dashboard.TREND_SPLIT_TEXT
    assert payload["trend_series"] == []
    assert payload["current"]["lines"] is None
    by_day = {row["day"]: row for row in payload["trend"]}
    assert by_day["2026-10-14"]["total"] == 0.7
    assert by_day["2026-10-14"]["usage"] is None


def test_one_old_block_in_the_window_turns_the_split_off(root: Path):
    """``current`` and ``previous`` can come from different files, so a window can mix a
    split block with an old one. The chart then draws the net for the whole window, since
    half a window of usage beside half of net would read as a fall in usage."""
    previous = september(daily={"2026-09-30": {"AmazonEC2": 0.6}})
    file(
        root,
        "2026-10-16",
        entry("2026-10-16", "2026-10", "delivery", current=split_october(), previous=previous),
    )

    payload = cost(root)

    assert payload["trend_split"] is False
    assert payload["trend_split_text"] == dashboard.TREND_SPLIT_TEXT


def test_no_trend_says_nothing_about_the_split(root: Path):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "no_export"))

    payload = cost(root)

    assert payload["trend"] == []
    assert payload["trend_split"] is False
    assert payload["trend_split_text"] is None


def test_the_projection_carries_the_usage_alone_beside_the_net(root: Path):
    """$9.80 of usage over the 14-day window, scaled to October's 31 days, is $21.70 of
    usage by month end, while ``month_end`` adds $3.50 of other lines to it."""
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=october()))

    projection = cost(root)["current"]["projection"]

    assert projection["month_end"] == 25.2
    assert projection["usage_month_end"] == pytest.approx(21.7)


@pytest.mark.parametrize(
    "change",
    [{"window_days": 0}, {"window_days": "x"}, {"usage_through": None}],
    ids=["zero days", "days not a number", "no usage figure"],
)
def test_a_projection_it_cannot_scale_has_no_usage_figure(root: Path, change: dict):
    current = october()
    current["projection"].update(change)
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=current))

    projection = cost(root)["current"]["projection"]

    assert projection["usage_month_end"] is None


def test_a_projection_too_early_to_make_has_no_usage_figure(root: Path):
    current = october(projection={"status": "too_early"})
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=current))

    assert cost(root)["current"]["projection"] == {"status": "too_early"}


def test_the_month_to_date_lines_put_usage_credits_and_net_apart(root: Path):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=split_october()))

    current = cost(root)["current"]

    assert current["lines"] == {
        "usage": pytest.approx(10.55),
        "credits": pytest.approx(-10.55),
        "other": pytest.approx(0.1),
        "net": pytest.approx(0.1),
    }
    assert [
        (row["key"], row["usage"], [p["product"] for p in row["products"]])
        for row in current["usage_by_series"]
    ] == [
        ("AmazonEC2", pytest.approx(10.15), ["AmazonEC2"]),
        ("AmazonS3", pytest.approx(0.4), ["AmazonS3"]),
    ]


def test_month_to_date_usage_folds_unslotted_products_into_other_services(root: Path):
    current = split_october()
    current["mtd"] = {
        **current["mtd"],
        "service": {
            "AmazonEC2": {"usage": 2.0, "other": -2.0},
            "AmazonCloudWatch": {"usage": 0.3, "other": 0.0},
            "AWSCostExplorer": {"usage": 0.5, "other": 0.0},
            "AmazonVPC": {"usage": 0.0, "other": 0.0},
        },
    }
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=current))

    rows = cost(root)["current"]["usage_by_series"]

    # VPC had no usage, so it draws no bar, and the fold names its products biggest first.
    assert [(row["key"], row["slot"], row["usage"]) for row in rows] == [
        ("AmazonEC2", 1, pytest.approx(2.0)),
        ("other_services", 4, pytest.approx(0.8)),
    ]
    assert rows[1]["products"] == [
        {"product": "AWSCostExplorer", "usage": pytest.approx(0.5)},
        {"product": "AmazonCloudWatch", "usage": pytest.approx(0.3)},
    ]


def test_a_split_of_the_wrong_types_reads_as_no_figure(root: Path):
    current = split_october(
        daily={"2026-10-15": {"AmazonEC2": 0.1}},
        daily_usage={"2026-10-15": {"AmazonEC2": "x", "AmazonS3": True}},
        daily_other={"2026-10-15": ["Credit"]},
    )
    current["mtd"]["line_type"] = {"Usage": "x", "Credit": float("inf")}
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=current))

    payload = cost(root)

    newest = payload["trend"][-1]
    assert newest["usage"] == 0.0 and newest["usage_by_series"] == {}
    assert newest["credits"] == 0.0 and newest["other_lines"] == 0.0
    assert payload["current"]["lines"] == {"usage": 0.0, "credits": 0.0, "other": 0.0, "net": 0.0}
    json.dumps(payload, allow_nan=False)


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
    # The chart's series runs through the size day, N - 1, so the partial 15th is left out,
    # and each day sums its storage classes. EarlyDelete is in GB-hours and adds nothing.
    assert bucket["size_series"] == [
        {"day": "2026-10-01", "gb": pytest.approx(110.0)},
        {"day": "2026-10-14", "gb": pytest.approx(115.7)},
    ]
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
    # The chart's series starts where the growth does, each day sized by its own month.
    assert bucket["size_series"] == [
        {"day": "2026-09-20", "gb": pytest.approx(90.0)},
        {"day": "2026-10-14", "gb": pytest.approx(93.1)},
    ]


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


def test_a_bucket_with_storage_only_on_the_size_day_has_no_growth(root: Path):
    """Growth needs a day before the size day. With none, the growth is absent, and the
    panel still reads rather than dividing by a span of zero days."""
    current = october(storage_daily={"example-lake": {"TimedStorage-ByteHrs": {"2026-10-14": 3.0}}})
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=current))

    payload = cost(root)

    assert payload["condition"] is None
    [bucket] = payload["s3"]
    assert bucket["size_gb"] == pytest.approx(93.0)
    assert bucket["growth"] is None


def test_growth_can_start_on_the_trends_first_day(root: Path):
    """The trend's first day is inside its window, so storage on 16 September, the first of
    the thirty days ending on the 15th, is where the growth starts."""
    previous = september(
        storage_daily={"example-lake": {"TimedStorage-ByteHrs": {"2026-09-16": 3.0}}}
    )
    current = october(storage_daily={"example-lake": {"TimedStorage-ByteHrs": {"2026-10-14": 3.1}}})
    file(
        root,
        "2026-10-16",
        entry("2026-10-16", "2026-10", "delivery", current=current, previous=previous),
    )

    payload = cost(root)

    assert payload["trend"][0]["day"] == "2026-09-16"
    [bucket] = payload["s3"]
    assert bucket["growth"]["from"] == "2026-09-16"
    assert bucket["growth"]["days"] == 28
