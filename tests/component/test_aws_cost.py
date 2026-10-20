"""The nightly read of the AWS cost and usage export, marketlake #913.

Every fixture is CUR-shaped Parquet built here with pyarrow from literals, never from the
constants under test, and served by ``tests/support/bucket.FakeS3``, whose listings carry a
``LastModified`` per version so the manifest barrier can be driven. The bucket, the account
id and every resource are fakes.

The worked examples are pass 7's of #911. October 2026 is the current period, the newest
usage day N is 15 October, and usage runs a flat $0.70 a day, so the complete window is the
1st through the 14th: $9.80 of usage over 14 days, scaled to October's 31 is $21.70, and
ten chains sessions in that window make $0.98 a session.
"""

from __future__ import annotations

import io
import json
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import aws_cost
from lake.config import BucketTarget, Config
from lake.report import ACTION, INFO
from tests.support.bucket import FakeS3

BUCKET = "example-billing"
PREFIX = "cur/marketlake-cost"
TARGET = BucketTarget(BUCKET, PREFIX)
ACCOUNT = "000000000000"

# 18:30 EDT on Friday 16 October 2026, which is 22:30 UTC, inside the October period.
NOW = datetime(2026, 10, 16, 22, 30, tzinfo=UTC)

# When the fake export landed: the data first, then the manifest a minute later.
DELIVERED = datetime(2026, 10, 16, 6, 0, tzinfo=UTC)
MANIFEST_AT = DELIVERED + timedelta(minutes=1)

# The columns a CUR 2.0 export carries, beyond the nine the figures read, so the read is
# shown to select its columns rather than take the file whole.
EXTRA = {
    "identity_line_item_id": pa.string(),
    "line_item_operation": pa.string(),
    "pricing_unit": pa.string(),
}


def line(
    start: datetime,
    product: str,
    usage_type: str,
    cost: float,
    *,
    resource: str = "",
    line_type: str = "Usage",
    amount: float = 0.0,
    currency: str = "USD",
    account: str = ACCOUNT,
) -> dict:
    """One CUR line, with its usage start as a UTC instant."""
    return {
        "line_item_usage_start_date": start,
        "line_item_product_code": product,
        "line_item_usage_type": usage_type,
        "line_item_resource_id": resource or None,
        "line_item_line_item_type": line_type,
        "line_item_unblended_cost": cost,
        "line_item_usage_amount": amount,
        "line_item_currency_code": currency,
        "line_item_usage_account_id": account,
        "identity_line_item_id": "x",
        "line_item_operation": "RunInstances",
        "pricing_unit": "Hrs",
    }


def parquet(rows: list[dict], *, start_type: pa.DataType | None = None) -> bytes:
    """A CUR-shaped Parquet file. ``start_type`` defaults to a zoned UTC timestamp.

    A naive type takes the same instants with their zone dropped, which is how a writer
    that stores UTC without saying so would write them.
    """
    start_type = start_type if start_type is not None else pa.timestamp("ms", tz="UTC")
    starts = [row["line_item_usage_start_date"] for row in rows]
    if pa.types.is_timestamp(start_type) and start_type.tz is None:
        starts = [value.astimezone(UTC).replace(tzinfo=None) for value in starts]
    elif not pa.types.is_timestamp(start_type):
        starts = [value.isoformat() for value in starts]
    fields = {
        "line_item_usage_start_date": pa.array(starts, start_type),
        "line_item_product_code": pa.array([r["line_item_product_code"] for r in rows]),
        "line_item_usage_type": pa.array([r["line_item_usage_type"] for r in rows]),
        "line_item_resource_id": pa.array([r["line_item_resource_id"] for r in rows], pa.string()),
        "line_item_line_item_type": pa.array([r["line_item_line_item_type"] for r in rows]),
        "line_item_unblended_cost": pa.array(
            [r["line_item_unblended_cost"] for r in rows], pa.float64()
        ),
        "line_item_usage_amount": pa.array(
            [r["line_item_usage_amount"] for r in rows], pa.float64()
        ),
        "line_item_currency_code": pa.array([r["line_item_currency_code"] for r in rows]),
        "line_item_usage_account_id": pa.array([r["line_item_usage_account_id"] for r in rows]),
    }
    for name, kind in EXTRA.items():
        fields[name] = pa.array([row[name] for row in rows], kind)
    if not rows:
        fields = {name: pa.array([], column.type) for name, column in fields.items()}
    buffer = io.BytesIO()
    pq.write_table(pa.table(fields), buffer)
    return buffer.getvalue()


def empty_schema() -> bytes:
    """A Parquet file with no columns and no rows, which AWS's "empty data" can be."""
    buffer = io.BytesIO()
    pq.write_table(pa.table({}), buffer)
    return buffer.getvalue()


def data_key(period: str, number: int = 1) -> str:
    return f"{PREFIX}/data/BILLING_PERIOD={period}/marketlake-cost-{number:05d}.snappy.parquet"


def manifest_key(period: str) -> str:
    return f"{PREFIX}/metadata/BILLING_PERIOD={period}/marketlake-cost-Manifest.json"


def deliver(
    s3: FakeS3,
    period: str,
    *chunks: bytes,
    data_at: datetime = DELIVERED,
    manifest_at: datetime | None = MANIFEST_AT,
) -> None:
    """Store a period's data files, then its manifest, as AWS writes them."""
    for number, chunk in enumerate(chunks, start=1):
        s3.store(data_key(period, number), chunk, last_modified=data_at)
    if manifest_at is not None:
        s3.store(manifest_key(period), b'{"not": "parsed"}', last_modified=manifest_at)


def source(s3: FakeS3):
    return lambda: (TARGET, s3)


def write_manifest(root: Path, *entries: tuple[str, str, str]) -> None:
    """A lake manifest naming ``(surface, ticker, day)`` partitions."""
    with open(root / "manifest.jsonl", "a", encoding="utf-8") as handle:
        for surface, ticker, day in entries:
            partition = f"{surface}/ticker={ticker}/date={day}.parquet"
            handle.write(json.dumps({"partition": partition, "rows": 1, "sha256": "0" * 64}))
            handle.write("\n")


# The ten October sessions in the complete window, 1 through 14 October 2026, written as
# literals. The 15th is N and the 16th is today, both outside the window.
OCTOBER_SESSIONS = (
    "2026-10-01",
    "2026-10-02",
    "2026-10-05",
    "2026-10-06",
    "2026-10-07",
    "2026-10-08",
    "2026-10-09",
    "2026-10-12",
    "2026-10-13",
    "2026-10-14",
)

# Twenty September sessions, which the closed month's figure divides by.
SEPTEMBER_SESSIONS = tuple(
    f"2026-09-{day:02d}"
    for day in (1, 2, 3, 4, 8, 9, 10, 11, 14, 15, 16, 17, 18, 21, 22, 23, 24, 25, 28, 29)
)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    lake = tmp_path / "lake"
    lake.mkdir()
    entries = [("chains", ticker, day) for day in OCTOBER_SESSIONS for ticker in ("SPY", "QQQ")]
    entries += [("chains", "SPY", "2026-10-15"), ("quotes", "SPY", "2026-10-03")]
    entries += [("chains", "SPY", day) for day in SEPTEMBER_SESSIONS]
    # A session before September, which neither month counts.
    entries += [("chains", "SPY", "2026-08-31")]
    write_manifest(lake, *entries)
    return lake


def at(day: int, hour: int = 12, month: int = 10) -> datetime:
    return datetime(2026, month, day, hour, tzinfo=UTC)


def october() -> list[dict]:
    """Pass 7's October: $0.70 of EC2 usage a day through the 14th, half of it on the 15th.

    Day 1's usage is split, with $0.30 of it at 02:00 UTC, which is still 30 September in New
    York, so a read that dated a zoned timestamp in its own zone would move it out of the
    period. Three other lines are added unscaled: tax, a fee and a credit, $3.50 in all.
    """
    rows = [line(at(1, 2), "AmazonEC2", "BoxUsage:t4g.small", 0.30, resource="i-0abc")]
    rows.append(line(at(1), "AmazonEC2", "BoxUsage:t4g.small", 0.40, resource="i-0abc"))
    rows += [
        line(at(day), "AmazonEC2", "BoxUsage:t4g.small", 0.70, resource="i-0abc")
        for day in range(2, 15)
    ]
    rows.append(line(at(15, 1), "AmazonEC2", "BoxUsage:t4g.small", 0.35, resource="i-0abc"))
    rows.append(line(at(1), "AmazonEC2", "", 1.00, line_type="Tax"))
    rows.append(line(at(1), "AmazonEC2", "", 3.00, line_type="Fee"))
    rows.append(line(at(3), "AmazonEC2", "", -0.50, line_type="Credit"))
    return rows


def september() -> list[dict]:
    """$0.60 of usage on each of September's 30 days, $18.00, the 1st's at 02:00 UTC."""
    rows = [line(at(1, 2, 9), "AmazonEC2", "BoxUsage:t4g.small", 0.60)]
    rows += [line(at(day, 12, 9), "AmazonEC2", "BoxUsage:t4g.small", 0.60) for day in range(2, 31)]
    rows.append(line(at(30, 12, 9), "AmazonEC2", "", 0.25, line_type="Tax"))
    return rows


def read(s3: FakeS3, root: Path, now: datetime = NOW) -> dict:
    return aws_cost.run(source(s3), root, now=now)


def tree(root: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


# -- the figures ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "start_type",
    [pa.timestamp("ms", tz="UTC"), pa.timestamp("us", tz="America/New_York"), pa.timestamp("ns")],
    ids=["zoned UTC", "zoned New York", "naive"],
)
def test_the_projection_and_cost_per_session_are_pass_sevens_worked_examples(
    root: Path, start_type: pa.DataType
):
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-10", parquet(october(), start_type=start_type))

    entry = read(s3, root)

    assert entry["state"] == "delivery" and entry["error"] is None
    current = entry["current"]
    assert current["newest_day"] == "2026-10-15"
    assert current["complete_through"] == "2026-10-14"
    # The 02:00 UTC row is the 1st's, whatever the column's zone.
    assert current["daily"]["2026-10-01"] == {"AmazonEC2": pytest.approx(4.70)}
    assert "2026-09-30" not in current["daily"]
    projection = current["projection"]
    assert projection["status"] == "estimate"
    assert projection["usage_through"] == pytest.approx(9.80)
    assert projection["window_days"] == 14
    assert projection["other"] == pytest.approx(3.50)
    # 9.80 / 14 * 31 = 21.70 of usage, plus 3.50 of other lines unscaled.
    assert projection["month_end"] == pytest.approx(25.20)
    per_session = current["cost_per_session"]
    assert per_session == {
        "status": "ok",
        "sessions": 10,
        "usage_cost": pytest.approx(9.80),
        "value": pytest.approx(0.98),
        "through": "2026-10-14",
    }
    assert current["sessions"] == 10
    # Month to date sums every line type, split into usage and other.
    assert current["mtd"]["service"]["AmazonEC2"] == {
        "usage": pytest.approx(10.15),
        "other": pytest.approx(3.50),
    }
    assert current["total"] == pytest.approx(13.65)


def test_the_daily_series_split_usage_from_every_other_line(root: Path):
    """``daily_usage`` and ``daily_other`` keep apart what ``daily`` nets (#921).

    The 1st carries $0.70 of usage beside $1.00 of tax and a $3.00 fee, and the 3rd carries
    $0.70 of usage beside a $0.50 credit, so the net alone would hide all four.
    """
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-09", parquet(september()))
    deliver(s3, "2026-10", parquet(october()))

    entry = read(s3, root)

    current = entry["current"]
    assert current["daily_usage"]["2026-10-01"] == {"AmazonEC2": pytest.approx(0.70)}
    assert current["daily_other"]["2026-10-01"] == {
        "Tax": pytest.approx(1.00),
        "Fee": pytest.approx(3.00),
    }
    assert current["daily_usage"]["2026-10-03"] == {"AmazonEC2": pytest.approx(0.70)}
    assert current["daily_other"]["2026-10-03"] == {"Credit": pytest.approx(-0.50)}
    # A day with only usage has no other line at all, rather than a zero.
    assert "2026-10-02" not in current["daily_other"]
    assert current["mtd"]["line_type"] == {
        "Usage": pytest.approx(10.15),
        "Tax": pytest.approx(1.00),
        "Fee": pytest.approx(3.00),
        "Credit": pytest.approx(-0.50),
    }
    previous = entry["previous"]
    assert previous["daily_usage"]["2026-09-30"] == {"AmazonEC2": pytest.approx(0.60)}
    assert previous["daily_other"] == {"2026-09-30": {"Tax": pytest.approx(0.25)}}
    # For every day of both blocks, usage plus the other lines is the net ``daily`` gives.
    for block in (current, previous):
        assert set(block["daily_usage"]) | set(block["daily_other"]) == set(block["daily"])
        for day, products in block["daily"].items():
            split = sum(block["daily_usage"].get(day, {}).values()) + sum(
                block["daily_other"].get(day, {}).values()
            )
            assert split == pytest.approx(sum(products.values())), day


def test_a_usage_start_that_is_not_a_timestamp_fails_the_read(root: Path):
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-10", parquet(october(), start_type=pa.string()))

    entry = read(s3, root)

    assert entry["state"] == "failed"
    assert entry["error"] == "the usage start column is not a timestamp"
    assert "current" not in entry


def test_the_first_of_the_month_is_too_early_for_either_figure(root: Path):
    s3 = FakeS3(BUCKET, versioning=None)
    rows = [line(at(1, 3), "AmazonEC2", "BoxUsage:t4g.small", 0.20)]
    deliver(s3, "2026-10", parquet(rows))

    current = read(s3, root, now=datetime(2026, 10, 1, 22, 30, tzinfo=UTC))["current"]

    assert current["newest_day"] == "2026-10-01"
    assert current["complete_through"] is None
    assert current["projection"] == {"status": "too_early"}
    assert current["cost_per_session"] == {"status": "too_early"}
    assert current["sessions"] is None


def test_a_window_with_no_captured_session_says_so(tmp_path: Path):
    lake = tmp_path / "lake"
    lake.mkdir()
    write_manifest(lake, ("quotes", "SPY", "2026-10-05"))
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-10", parquet(october()))

    per_session = read(s3, lake)["current"]["cost_per_session"]

    assert per_session["status"] == "no_sessions"
    assert per_session["sessions"] == 0
    assert "value" not in per_session


def test_the_closed_month_uses_all_of_its_days(root: Path):
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-09", parquet(september()))
    deliver(s3, "2026-10", parquet(october()))

    previous = read(s3, root)["previous"]

    assert previous["period"] == "2026-09"
    assert previous["total"] == pytest.approx(18.25)
    # $18.00 of usage over twenty sessions, the tax left out.
    assert previous["cost_per_session"] == {
        "status": "ok",
        "sessions": 20,
        "usage_cost": pytest.approx(18.00),
        "value": pytest.approx(0.90),
        "through": "2026-09-30",
    }
    assert previous["daily"]["2026-09-01"] == {"AmazonEC2": pytest.approx(0.60)}
    assert previous["delivered_at"] == MANIFEST_AT.isoformat()


def test_the_closed_month_counts_a_session_on_its_last_day(root: Path):
    """A session captured on 30 September is September's, so the closed month divides by
    twenty-one sessions, not the twenty before its last day."""
    write_manifest(root, ("chains", "SPY", "2026-09-30"))
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-09", parquet(september()))
    deliver(s3, "2026-10", parquet(october()))

    previous = read(s3, root)["previous"]

    assert previous["sessions"] == 21
    assert previous["cost_per_session"]["sessions"] == 21
    assert previous["cost_per_session"]["value"] == pytest.approx(18.00 / 21)


def test_the_second_of_the_month_has_a_one_day_window(root: Path):
    """On the 2nd, N is the 2nd and the 1st is complete, so both figures read from a window of
    one day rather than waiting a day longer."""
    rows = [
        line(at(1), "AmazonEC2", "BoxUsage:t4g.small", 0.70),
        line(at(2), "AmazonEC2", "BoxUsage:t4g.small", 0.30),
    ]
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-10", parquet(rows))

    current = read(s3, root, now=datetime(2026, 10, 2, 22, 30, tzinfo=UTC))["current"]

    assert current["complete_through"] == "2026-10-01"
    projection = current["projection"]
    assert projection["status"] == "estimate"
    assert projection["window_days"] == 1
    # 0.70 over one day, scaled to October's 31.
    assert projection["month_end"] == pytest.approx(21.70)
    assert current["cost_per_session"] == {
        "status": "ok",
        "sessions": 1,
        "usage_cost": pytest.approx(0.70),
        "value": pytest.approx(0.70),
        "through": "2026-10-01",
    }


def test_one_captured_session_is_a_figure(tmp_path: Path):
    """A window holding a single chains session divides by one, and only a window with none
    has no figure."""
    lake = tmp_path / "lake"
    lake.mkdir()
    write_manifest(lake, ("chains", "SPY", "2026-10-05"))
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-10", parquet(october()))

    per_session = read(s3, lake)["current"]["cost_per_session"]

    assert per_session["status"] == "ok"
    assert per_session["sessions"] == 1
    assert per_session["value"] == pytest.approx(9.80)


@pytest.mark.parametrize("line_type", ["DiscountedUsage", "SavingsPlanCoveredUsage"])
def test_discounted_and_savings_plan_lines_are_usage(root: Path, line_type: str):
    """A line a reservation or a Savings Plan covers is still usage, so it is scaled with the
    rest. Read as an other line it would be added once, unscaled, and the projection would
    fall."""
    rows = october()
    # Two of the $0.70 days, the 3rd and the 4th, billed under the covered line type.
    for row in rows[3:5]:
        row["line_item_line_item_type"] = line_type
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-10", parquet(rows))

    current = read(s3, root)["current"]

    projection = current["projection"]
    assert projection["usage_through"] == pytest.approx(9.80)
    assert projection["other"] == pytest.approx(3.50)
    assert projection["month_end"] == pytest.approx(25.20)
    assert current["mtd"]["service"]["AmazonEC2"]["usage"] == pytest.approx(10.15)


def test_a_null_currency_credit_beside_dollars_is_one_currency(root: Path):
    """AWS leaves the currency empty on some lines. An empty currency is not a second one, so
    it must not refuse the period, and the period's currency is the one its lines name."""
    rows = october() + [line(at(5), "AmazonEC2", "", 0.0, line_type="Credit", currency=None)]
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-10", parquet(rows))

    entry = read(s3, root)

    assert entry["state"] == "delivery"
    assert entry["error"] is None
    assert entry["currency"] == "USD"


def test_a_delivery_records_its_currency(root: Path):
    """The page labels every figure with the currency the export bills in."""
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-10", parquet(october()))

    assert read(s3, root)["currency"] == "USD"


def test_an_s3_line_with_no_resource_is_no_bucket(root: Path):
    """A tax line under the S3 product names no bucket. It counts in the month's total and
    stays out of the per-bucket split, where it would show as a bucket with no name."""
    rows = october() + [line(at(5), "AmazonS3", "", 0.20, line_type="Tax")]
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-10", parquet(rows))

    current = read(s3, root)["current"]

    assert current["mtd"]["s3"] == {}
    assert current["total"] == pytest.approx(13.85)


def test_an_account_only_the_closed_month_names_is_masked(root: Path):
    """The mask takes its account ids from both periods. An account that billed in September
    and not in October would otherwise reach the file in the closed month's bucket names."""
    other = "111111111111"
    rows = september() + [
        line(
            at(10, 12, 9),
            "AmazonS3",
            "TimedStorage-ByteHrs",
            0.10,
            resource=f"example-lake-{other}",
            amount=1.0,
            account=other,
        )
    ]
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-09", parquet(rows))
    deliver(s3, "2026-10", parquet(october()))

    entry = read(s3, root)

    assert set(entry["previous"]["storage_daily"]) == {"example-lake-<account>"}
    assert other not in json.dumps(entry)


def test_a_non_usage_line_dated_the_last_day_does_not_move_n(root: Path):
    """N is the newest usage day. A $0 tax line dated 31 October would move N to the 31st,
    the window to 30 days and the projection with it, were N taken over every line."""
    s3 = FakeS3(BUCKET, versioning=None)
    rows = october() + [line(at(31), "AmazonEC2", "", 0.0, line_type="Tax")]
    deliver(s3, "2026-10", parquet(rows))

    current = read(s3, root)["current"]

    assert current["newest_day"] == "2026-10-15"
    assert current["complete_through"] == "2026-10-14"
    assert current["projection"]["window_days"] == 14
    assert current["projection"]["month_end"] == pytest.approx(25.20)
    assert current["cost_per_session"]["value"] == pytest.approx(0.98)
    assert current["sessions"] == 10


def test_a_period_with_no_usage_line_is_too_early_for_either_figure(root: Path):
    """A delivery holding only a fee and a credit has no usage day, so it has no N."""
    s3 = FakeS3(BUCKET, versioning=None)
    rows = [
        line(at(1), "AmazonEC2", "", 3.00, line_type="Fee"),
        line(at(5), "AmazonEC2", "", -0.50, line_type="Credit"),
    ]
    deliver(s3, "2026-10", parquet(rows))

    current = read(s3, root)["current"]

    assert current["newest_day"] is None
    assert current["complete_through"] is None
    assert current["projection"] == {"status": "too_early"}
    assert current["cost_per_session"] == {"status": "too_early"}
    assert current["total"] == pytest.approx(2.50)


@pytest.mark.parametrize(
    ("column", "value", "reason"),
    [
        ("cost", float("nan"), "a billing period's cost sums to a number that is not finite"),
        ("cost", float("inf"), "a billing period's cost sums to a number that is not finite"),
        (
            "amount",
            float("nan"),
            "a billing period's usage amount sums to a number that is not finite",
        ),
    ],
    ids=["a NaN cost", "an infinite cost", "a NaN amount"],
)
def test_a_period_whose_sums_are_not_finite_fails_the_read(
    root: Path, column: str, value: float, reason: str
):
    """A NaN in the file would reach ``/api/cost`` as a bare ``NaN``, which the page's
    ``JSON.parse`` refuses, so the read refuses it first, and the file still writes."""
    s3 = FakeS3(BUCKET, versioning=None)
    bad = (
        line(at(5), "AmazonEC2", "BoxUsage:t4g.small", value)
        if column == "cost"
        else line(at(5), "AmazonS3", "TimedStorage-ByteHrs", 0.01, amount=value)
    )
    deliver(s3, "2026-10", parquet([*october(), bad]))

    entry = read(s3, root)

    assert entry["state"] == "failed"
    assert entry["error"] == reason
    assert "current" not in entry
    path = aws_cost.write_cost_report(root, entry, now=NOW, pid=4242)
    assert json.loads(path.read_text()) == entry


def test_s3_is_split_per_bucket_by_raw_usage_type_and_storage_is_carried_per_day(root: Path):
    rows = october()
    for day in (13, 14):
        rows.append(
            line(
                at(day),
                "AmazonS3",
                "TimedStorage-ByteHrs",
                0.08,
                resource="example-lake",
                amount=50 / 31,
            )
        )
    rows += [
        line(
            at(14),
            "AmazonS3",
            "TimedStorage-SIA-ByteHrs",
            0.01,
            resource="example-lake",
            amount=0.5,
        ),
        line(at(14), "AmazonS3", "EarlyDelete-SIA", 0.02, resource="example-lake", amount=99.0),
        line(
            at(14),
            "AmazonS3",
            "USE2-TimedStorage-ByteHrs",
            0.01,
            resource="example-other",
            amount=0.25,
        ),
        line(at(14), "AmazonS3", "Requests-Tier1", 0.05, resource="example-lake", amount=10000),
        line(at(14), "AmazonS3", "DataTransfer-Out-Bytes", 0.03, resource="example-lake"),
        line(at(14), "AmazonEC2", "DataTransfer-Out-Bytes", 0.04, resource="i-0abc"),
    ]
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-10", parquet(rows))

    current = read(s3, root)["current"]

    assert current["mtd"]["s3"]["example-lake"] == {
        "TimedStorage-ByteHrs": pytest.approx(0.16),
        "TimedStorage-SIA-ByteHrs": pytest.approx(0.01),
        "EarlyDelete-SIA": pytest.approx(0.02),
        "Requests-Tier1": pytest.approx(0.05),
        "DataTransfer-Out-Bytes": pytest.approx(0.03),
    }
    # A transfer type of one name under two products stays apart.
    assert current["mtd"]["usage_type"]["AmazonEC2"]["DataTransfer-Out-Bytes"] == pytest.approx(
        0.04
    )
    assert current["mtd"]["usage_type"]["AmazonS3"]["DataTransfer-Out-Bytes"] == pytest.approx(0.03)
    # Only TimedStorage lines are a size. EarlyDelete is GB-hours and is left out.
    assert current["storage_daily"] == {
        "example-lake": {
            "TimedStorage-ByteHrs": {
                "2026-10-13": pytest.approx(50 / 31),
                "2026-10-14": pytest.approx(50 / 31),
            },
            "TimedStorage-SIA-ByteHrs": {"2026-10-14": pytest.approx(0.5)},
        },
        "example-other": {"USE2-TimedStorage-ByteHrs": {"2026-10-14": pytest.approx(0.25)}},
    }


def test_account_ids_are_masked_in_arns_and_in_bucket_names(root: Path):
    rows = october() + [
        line(
            at(14),
            "AmazonCloudWatch",
            "TimedStorage-ByteHrs",
            0.01,
            resource=f"arn:aws:logs:us-east-1:{ACCOUNT}:log-group:/marketlake/x",
        ),
        line(
            at(14),
            "AmazonS3",
            "TimedStorage-ByteHrs",
            0.02,
            resource=f"example-lake-{ACCOUNT}",
            amount=1.0,
        ),
        line(
            at(14),
            "AmazonEC2",
            "EBS:SnapshotUsage",
            0.03,
            resource=f"arn:aws:ec2:us-east-1:{ACCOUNT}:snapshot/snap-0abc",
        ),
    ]
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-10", parquet(rows))

    entry = read(s3, root)

    resources = entry["current"]["mtd"]["resource"]
    assert set(resources) == {
        "i-0abc",
        "arn:aws:logs:us-east-1:<account>:log-group:/marketlake/x",
        "example-lake-<account>",
        "arn:aws:ec2:us-east-1:<account>:snapshot/snap-0abc",
    }
    assert set(entry["current"]["mtd"]["s3"]) == {"example-lake-<account>"}
    assert set(entry["current"]["storage_daily"]) == {"example-lake-<account>"}
    assert ACCOUNT not in json.dumps(entry)


def test_mask_resource_reads_only_a_twelve_digit_fifth_field():
    assert aws_cost.mask_resource("arn:aws:s3:::example-lake", []) == "arn:aws:s3:::example-lake"
    assert (
        aws_cost.mask_resource("arn:aws:iam::111111111111:role/x", [])
        == "arn:aws:iam::<account>:role/x"
    )
    assert aws_cost.mask_resource("arn:aws:iam::12345:role/x", []) == "arn:aws:iam::12345:role/x"
    assert aws_cost.mask_resource("vol-0123456789012", []) == "vol-0123456789012"


def test_two_currencies_in_one_period_fail_the_read(root: Path):
    rows = october() + [line(at(14), "AmazonEC2", "BoxUsage", 1.0, currency="EUR")]
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-10", parquet(rows))

    entry = read(s3, root)

    assert entry["state"] == "failed"
    assert entry["error"] == "a billing period holds more than one currency"


# -- the chunks ----------------------------------------------------------------------------


def test_zero_row_and_empty_schema_chunks_are_dropped(root: Path):
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-10", parquet(october()))
    alone = read(s3, root)["current"]

    s3 = FakeS3(BUCKET, versioning=None)
    deliver(
        s3, "2026-10", parquet(october()[:8]), parquet([]), empty_schema(), parquet(october()[8:])
    )
    chunked = read(s3, root)["current"]

    assert chunked == alone


def test_a_delivery_of_only_empty_chunks_is_a_delivery_of_nothing(root: Path):
    """AWS delivers an empty file for a period with no lines yet. The manifest says the
    delivery is whole, so the period reads as a delivery that cost nothing, not as no
    delivery at all."""
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-10", parquet([]))

    entry = read(s3, root)

    assert entry["state"] == "delivery"
    assert entry["current"]["total"] == 0.0
    assert entry["current"]["projection"] == {"status": "too_early"}


def test_a_listing_longer_than_one_page_is_read_whole(root: Path):
    """S3 lists a thousand keys a page. A read that took only the first page would drop every
    later chunk and report a smaller month."""
    rows = october()
    s3 = FakeS3(BUCKET, versioning=None, page_size=1)
    deliver(s3, "2026-10", parquet(rows[:8]), parquet(rows[8:]))

    current = read(s3, root)["current"]

    assert current["total"] == pytest.approx(13.65)
    gets = [kwargs["Key"] for name, kwargs in s3.calls if name == "get_object"]
    assert gets == [data_key("2026-10", 1), data_key("2026-10", 2)]


def test_a_chunk_whose_footer_cannot_be_read_fails_the_read(root: Path):
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-10", parquet(october()), b"PAR1 this is not a footer")

    entry = read(s3, root)

    assert entry["state"] == "failed"
    assert entry["error"] == "a data file's Parquet footer could not be read"


# -- the states ----------------------------------------------------------------------------


def test_a_bucket_with_no_delivery_at_all_is_no_export(root: Path):
    entry = read(FakeS3(BUCKET, versioning=None), root)

    assert entry["state"] == "no_export"
    assert entry["period"] == "2026-10"
    assert "current" not in entry and "previous" not in entry
    assert aws_cost.report_line(entry) == (
        "aws cost read: no export has been delivered yet",
        INFO,
    )


def test_the_first_of_a_month_is_no_delivery_with_the_closed_month_beside_it(root: Path):
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-09", parquet(september()))

    entry = read(s3, root)

    assert entry["state"] == "no_delivery"
    assert "current" not in entry
    assert entry["previous"]["period"] == "2026-09"
    assert aws_cost.report_line(entry) == ("aws cost read: no delivery yet for 2026-10", INFO)


def test_a_first_month_delivery_writes_no_previous_block(root: Path):
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-10", parquet(october()))

    entry = read(s3, root)

    assert entry["state"] == "delivery"
    assert "previous" not in entry
    assert aws_cost.report_line(entry) is None


def test_a_data_file_newer_than_its_manifest_is_refreshing(root: Path):
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-10", parquet(october()), data_at=MANIFEST_AT + timedelta(seconds=1))

    entry = read(s3, root)

    assert entry["state"] == "refreshing"
    assert "current" not in entry
    # The listing alone refuses it, so nothing is downloaded.
    assert [name for name, _ in s3.calls if name == "get_object"] == []
    assert aws_cost.report_line(entry) == (
        "aws cost read: the 2026-10 export was refreshing, read again next night",
        INFO,
    )


def test_data_with_no_manifest_yet_is_refreshing(root: Path):
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-10", parquet(october()), manifest_at=None)

    assert read(s3, root)["state"] == "refreshing"


def test_a_manifest_that_changes_during_the_download_is_refreshing(root: Path):
    def refresh(kwargs):
        if kwargs["Key"].startswith(f"{PREFIX}/data/BILLING_PERIOD=2026-10/"):
            s3.store(manifest_key("2026-10"), b"{}", last_modified=MANIFEST_AT)

    s3 = FakeS3(BUCKET, versioning=None, on_get=refresh)
    deliver(s3, "2026-10", parquet(october()))

    assert read(s3, root)["state"] == "refreshing"


def test_a_manifest_rewritten_at_the_same_size_during_the_download_is_refreshing(root: Path):
    """A manifest rewritten with the same bytes changes only its ``LastModified``, so the
    barrier compares the time too, or the rewrite goes unseen."""

    def refresh(kwargs):
        if kwargs["Key"].startswith(f"{PREFIX}/data/BILLING_PERIOD=2026-10/"):
            s3.store(
                manifest_key("2026-10"),
                b'{"not": "parsed"}',
                last_modified=datetime(2026, 10, 16, 6, 1, 1, tzinfo=UTC),
            )

    s3 = FakeS3(BUCKET, versioning=None, on_get=refresh)
    deliver(s3, "2026-10", parquet(october()))

    assert read(s3, root)["state"] == "refreshing"


def test_data_written_in_the_manifests_own_second_is_a_delivery(root: Path):
    """S3 stamps ``LastModified`` to the second, so a data file and the manifest written
    after it can share one. Only data strictly newer than the manifest is a refresh, or
    such a delivery would read as refreshing every night."""
    s3 = FakeS3(BUCKET, versioning=None)
    same_second = datetime(2026, 10, 16, 6, 1, tzinfo=UTC)
    deliver(s3, "2026-10", parquet(october()), data_at=same_second, manifest_at=same_second)

    entry = read(s3, root)

    assert entry["state"] == "delivery"
    assert entry["current"]["total"] == pytest.approx(13.65)


def test_a_data_file_served_newer_than_the_manifest_is_refreshing(root: Path):
    """The listing was older than the manifest, and the object was rewritten before its GET."""

    def rewrite(kwargs):
        key = kwargs["Key"]
        body = s3.body(key)
        s3.store(key, body, last_modified=MANIFEST_AT + timedelta(seconds=5))

    s3 = FakeS3(BUCKET, versioning=None, on_get=rewrite)
    deliver(s3, "2026-10", parquet(october()))

    assert read(s3, root)["state"] == "refreshing"


def test_a_refreshing_previous_period_omits_its_block_and_keeps_the_state(root: Path):
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-09", parquet(september()), data_at=MANIFEST_AT + timedelta(hours=1))
    deliver(s3, "2026-10", parquet(october()))

    entry = read(s3, root)

    assert entry["state"] == "delivery"
    assert "previous" not in entry


@pytest.mark.parametrize(
    ("current", "state"),
    [(True, "delivery"), (False, "no_delivery")],
    ids=["beside a delivery", "beside no delivery"],
)
def test_a_previous_period_that_fails_to_read_omits_its_block_and_keeps_the_state(
    root: Path, current: bool, state: str
):
    """A corrupt chunk in last month's re-delivery must not blank this month's figures for
    the two weeks AWS keeps re-delivering it. The file names the failure apart."""
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-09", parquet(september()), b"PAR1 this is not a footer")
    if current:
        deliver(s3, "2026-10", parquet(october()))

    entry = read(s3, root)

    assert entry["state"] == state
    assert entry["error"] is None
    assert "previous" not in entry
    assert entry["previous_error"] == "a data file's Parquet footer could not be read"
    assert ("current" in entry) is current
    if current:
        assert entry["current"]["projection"]["month_end"] == pytest.approx(25.20)


def test_a_previous_period_the_grant_refuses_is_named_by_type(root: Path):
    s3 = FakeS3(
        BUCKET, versioning=None, deny_prefixes=(f"{PREFIX}/metadata/BILLING_PERIOD=2026-09/",)
    )
    deliver(s3, "2026-10", parquet(october()))

    entry = read(s3, root)

    assert entry["state"] == "delivery"
    assert entry["previous_error"] == "ClientError"
    assert entry["current"]["total"] == pytest.approx(13.65)


def test_a_previous_period_whose_figures_fail_omits_its_block_too(root: Path, monkeypatch):
    """The closed month's figures are part of reading it, so a raise while computing them is
    contained the same way, and the current month's figures still land."""

    class _Unexpected(Exception):
        pass

    def raises(read, *, sessions, accounts):
        raise _Unexpected("a message that names example-billing")

    monkeypatch.setattr(aws_cost, "previous_figures", raises)
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-09", parquet(september()))
    deliver(s3, "2026-10", parquet(october()))

    entry = read(s3, root)

    assert entry["state"] == "delivery"
    assert entry["previous_error"] == "_Unexpected"
    assert "previous" not in entry
    assert entry["current"]["total"] == pytest.approx(13.65)


def test_a_previous_period_that_reads_carries_no_error_field(root: Path):
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-09", parquet(september()))
    deliver(s3, "2026-10", parquet(october()))

    entry = read(s3, root)

    assert "previous_error" not in entry
    assert entry["previous"]["period"] == "2026-09"


def test_the_manifest_is_listed_and_never_asked_for_by_key(root: Path):
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-09", parquet(september()))
    deliver(s3, "2026-10", parquet(october()))

    read(s3, root)

    names = [name for name, _ in s3.calls]
    assert "head_object" not in names
    gets = [kwargs["Key"] for name, kwargs in s3.calls if name == "get_object"]
    assert gets == [data_key("2026-10"), data_key("2026-09")]
    lists = [kwargs["Prefix"] for name, kwargs in s3.calls if name == "list_objects_v2"]
    # Each period's manifest is listed before its download and again after.
    assert lists == [
        f"{PREFIX}/metadata/BILLING_PERIOD=2026-10/",
        f"{PREFIX}/data/BILLING_PERIOD=2026-10/",
        f"{PREFIX}/metadata/BILLING_PERIOD=2026-10/",
        f"{PREFIX}/metadata/BILLING_PERIOD=2026-09/",
        f"{PREFIX}/data/BILLING_PERIOD=2026-09/",
        f"{PREFIX}/metadata/BILLING_PERIOD=2026-09/",
    ]


def test_a_removed_grant_is_a_failed_read_named_by_type(root: Path):
    s3 = FakeS3(BUCKET, versioning=None, deny_prefixes=(f"{PREFIX}/",))
    deliver(s3, "2026-10", parquet(october()))

    entry = read(s3, root)

    assert entry["state"] == "failed"
    assert entry["error"] == "ClientError"
    assert aws_cost.report_line(entry) == ("aws cost read failed: ClientError", ACTION)
    assert BUCKET not in json.dumps(entry)


def test_the_period_is_the_utc_month_of_the_now_it_is_handed(root: Path):
    """23:30 EDT on 31 October is 03:30 UTC on 1 November, so the period is November, and the
    file is still filed under the Eastern night of the run."""
    late = datetime(2026, 11, 1, 3, 30, tzinfo=UTC)
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-10", parquet(october()))

    entry = read(s3, root, now=late)

    assert entry["period"] == "2026-11"
    assert entry["day"] == "2026-10-31"
    assert entry["state"] == "no_delivery"
    assert entry["previous"]["period"] == "2026-10"


def test_the_read_leaves_the_lake_untouched(root: Path):
    s3 = FakeS3(BUCKET, versioning=None)
    deliver(s3, "2026-09", parquet(september()))
    deliver(s3, "2026-10", parquet(october()))
    before = tree(root)

    read(s3, root)

    assert tree(root) == before


# -- the source ------------------------------------------------------------------------------


def _config(**changes) -> Config:
    return Config.from_mapping(
        {
            "lake_root": "/srv/lake",
            "backup_target": "s3://example-backup/lake",
            "healthchecks_ping_key": "k",
            "ntfy_topic": "t",
            "schwab_api_key": "a",
            "schwab_app_secret": "s",
            "bucket_credentials": "instance_profile",
            "bucket_region": "us-east-1",
            **changes,
        }
    )


def test_a_config_with_no_cost_export_has_no_source():
    assert aws_cost.client_source(_config()) is None


def test_a_cost_export_that_is_a_path_fails_the_read_not_the_load(root: Path, monkeypatch):
    config = _config(cost_export="/srv/not-a-bucket")
    assert config.cost_export == "/srv/not-a-bucket"

    def no_connect(*args, **kwargs):
        raise AssertionError("connect was called for a path")

    monkeypatch.setattr("lake.bucket.connect", no_connect)
    entry = aws_cost.run(aws_cost.client_source(config), root, now=NOW)

    assert entry["state"] == "failed"
    assert entry["error"] == "cost_export is not an s3:// location"


def test_the_source_connects_through_bucket_connect_with_the_parsed_target(monkeypatch):
    seen = []

    def connect(config, target):
        seen.append(target)
        return target, "client"

    monkeypatch.setattr("lake.bucket.connect", connect)
    build = aws_cost.client_source(_config(cost_export=f"s3://{BUCKET}/{PREFIX}"))

    assert seen == []
    assert build() == (TARGET, "client")
    assert seen == [TARGET]


# -- the file ------------------------------------------------------------------------------


def test_the_report_file_is_written_once_under_the_eastern_night(root: Path):
    entry = read(FakeS3(BUCKET, versioning=None), root)

    path = aws_cost.write_cost_report(root, entry, now=NOW, pid=4242)

    assert path == root / "reports" / "aws_cost" / "date=2026-10-16" / "183000000000-4242.json"
    text = path.read_text()
    assert json.loads(text) == entry
    assert text == json.dumps(entry, sort_keys=True) + "\n"
    with pytest.raises(FileExistsError):
        aws_cost.write_cost_report(root, entry, now=NOW, pid=4242)
    second = aws_cost.write_cost_report(root, entry, now=NOW + timedelta(microseconds=1), pid=4242)
    assert re.fullmatch(r"183000000001-4242\.json", second.name)


def test_a_file_written_after_utc_midnight_lands_under_the_eastern_night(root: Path):
    """00:30 UTC on 1 November is 20:30 EDT on 31 October, the night the sweep ran, so the
    file's directory is the 31st's, where the UTC date would put it a day ahead."""
    late = datetime(2026, 11, 1, 0, 30, tzinfo=UTC)

    path = aws_cost.write_cost_report(root, {"state": "failed"}, now=late, pid=4242)

    assert path == root / "reports" / "aws_cost" / "date=2026-10-31" / "203000000000-4242.json"


def test_a_failed_read_after_utc_midnight_names_the_utc_period(root: Path):
    """A failed entry names the period the read would have read, which is the UTC month, so
    at 20:30 EDT on 31 October it is November, as a read that worked would say."""

    def broken():
        raise RuntimeError("the source could not be built")

    entry = aws_cost.run(broken, root, now=datetime(2026, 11, 1, 0, 30, tzinfo=UTC))

    assert entry["state"] == "failed"
    assert entry["error"] == "RuntimeError"
    assert entry["period"] == "2026-11"
    assert entry["day"] == "2026-10-31"


def test_the_writer_refuses_a_missing_lake_root(tmp_path: Path):
    missing = tmp_path / "gone"
    with pytest.raises(FileNotFoundError):
        aws_cost.write_cost_report(missing, {"state": "no_export"}, now=NOW)
    assert not missing.exists()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_the_writer_refuses_a_number_json_cannot_hold_and_leaves_no_file(root: Path, value: float):
    entry = {"state": "delivery", "current": {"total": value}}

    with pytest.raises(ValueError):
        aws_cost.write_cost_report(root, entry, now=NOW, pid=4242)

    assert list((root / "reports" / "aws_cost").rglob("*.json")) == []


def test_a_failed_entry_carries_error_and_never_problems(root: Path):
    entry = read(FakeS3(BUCKET, versioning=None, deny_prefixes=(PREFIX,)), root)

    assert set(entry) == {"at", "day", "period", "state", "error", "currency"}
    assert entry["at"] == "2026-10-16T18:30:00-04:00"


def test_the_period_helpers():
    assert aws_cost.period_of(date(2026, 12, 31)) == "2026-12"
    assert aws_cost.previous_period("2026-01") == "2025-12"
    assert aws_cost.days_in_period("2028-02") == 29
