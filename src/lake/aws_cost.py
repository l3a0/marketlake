"""The nightly read of the AWS cost and usage export, and the report file it writes.

The hosted lake runs on AWS, and until marketlake #911 nothing showed what it cost. The
owner chose AWS Data Exports, the cost and usage report in its CUR 2.0 form, which AWS
delivers as Parquet into a billing bucket and refreshes up to three times a day.
Marketlake #912 builds the export. This module is the read, marketlake #913: the evening
sweep calls :func:`run` once a night on a host whose ``config.yaml`` names the export in
``cost_export``, and the dashboard's Cost panel reads the file it writes.

**What one read does.**

1. It reads two billing periods, which are UTC calendar months: the one holding the sweep's
   own ``now`` and the one before it. The sweep hands in the ``now`` it already read, so a
   late run on a month's last day cannot cross into the next period.
2. For each period it lists ``metadata/BILLING_PERIOD=YYYY-MM/`` and takes the manifest's
   ``LastModified`` from that listing. It never asks for the manifest by key, because a
   ``HEAD`` on a missing key can answer AccessDenied rather than 404 under a list grant
   conditioned on a prefix, which would turn the first day of every month into a failure.
   The manifest's JSON shape is documented nowhere, so the read does not parse it.
3. It lists ``data/BILLING_PERIOD=YYYY-MM/`` and downloads only keys that listing returned,
   into a temp directory outside the lake. A data file newer than the manifest, or a
   manifest that changed between the listing before the download and the one after it,
   means a refresh was writing, and the read waits for the next night rather than mix two
   deliveries.
4. It drops any file whose Parquet footer reports zero rows, because AWS overwrites a
   shrinking month's leftover chunks "with empty data" and an empty-schema file breaks a
   reader. A file whose footer cannot be read fails the read.
5. It aggregates with pyarrow's ``Table.group_by``. pyarrow rather than DuckDB, because the
   sweep has no reason to import the dashboard's DuckDB layer, importing it raised peak RSS
   from 15.6 MB to 117 MB, and DuckDB dates a zoned timestamp in the process's time zone.

**The UTC day comes from the column's type.** AWS says only that the usage start is a
timestamp in UTC. A zoned timestamp is cast to UTC before its date is taken, since pyarrow
dates a zoned timestamp in its own zone, a naive one is read as UTC, and any other type
refuses the read.

**The raw export never enters the lake.** The file this writes holds only the summaries the
panel draws, under ``reports/aws_cost/date=D/``. Resource IDs are masked first: an ARN's
twelve-digit account segment becomes ``<account>``, and so does any exact occurrence of an
account id the export's ``line_item_usage_account_id`` names, which is held in memory only.
A bucket name cannot hold ``:``, so the ARN rule never touches one, and the second rule
covers a bucket named after the account.

**The figures**, each defined so two builders compute the same number. N is the newest
usage day in the period's delivery, taken from the usage lines alone, since a tax or credit
line can be dated the period's last day. N is partial, so the complete window runs from the
period's first day through N − 1, counted in calendar days. A period whose delivery holds
no usage line has no N, and both figures read ``too_early``.

1. The projection scales the usage lines, ``Usage``, ``DiscountedUsage`` and
   ``SavingsPlanCoveredUsage``, from that window to the whole month, and adds every other
   line unscaled, since a recurring fee posts once on the first day and scaling it would
   multiply it. It is ``too_early`` while N − 1 falls before the period.
2. Cost per captured session is that window's usage cost over the distinct chains days the
   lake's manifest records in the same window. It is ``too_early`` under the projection's
   rule and ``no_sessions`` at zero. The closed month's figure uses all of its days.
3. The storage series carries each bucket's ``TimedStorage-*`` amount per usage type per
   day, in GB-months. The dashboard turns one day's amount into a billed size by the
   month's days. ``EarlyDelete-*`` is in GB-hours, so it is left out.

The figures sum ``line_item_unblended_cost``. ``line_item_net_unblended_cost`` exists only on
a month with a discount, so nothing here names it. A period with more than one
``line_item_currency_code`` is refused, because summing two currencies gives a number in
neither.

**Five states.** A file names one at its top level, beside the current period.

1. ``no_export``: ``data/`` lists no ``BILLING_PERIOD=`` prefix at all, which is every night
   before the first delivery.
2. ``no_delivery``: the export exists and the current period has none yet, which is the 1st
   of a month.
3. ``delivery``: the ``current`` block carries the figures.
4. ``refreshing``: the barrier refused, so the read waits for the next night.
5. ``failed``: ``error`` names an exception's type, or one of this module's own reasons,
   which name no bucket and no key.

The field is ``error`` and never ``problems``, which in the nightly files means "withheld
the ping". A ``previous`` block rides along whenever the previous period reads, whatever the
current state, so the closed month still shows on the 1st. A previous period with no
delivery, which is every night of the export's first month, or one whose barrier refused,
omits the block and never changes the state. So does a previous period whose read fails,
and the file then names the failure in a top-level ``previous_error``, as ``error`` names
one, because AWS re-delivers a closed month for up to two weeks and a corrupt chunk there
must not blank the current month's figures for as long.

**Every number the file holds is finite.** A period whose cost or usage amount sums to NaN
or an infinity is refused as ``failed``, and the writer encodes with ``allow_nan`` off, since
a bare ``NaN`` in the file would reach ``/api/cost`` and the page's ``JSON.parse`` refuses
it.
"""

from __future__ import annotations

import calendar as month_calendar
import json
import math
import os
import re
import tempfile
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from lake.calendar import MARKET_TZ
from lake.config import BucketTarget, Config, parse_backup_target
from lake.manifest import read_manifest
from lake.paths import AWS_COST_DIR, CHAINS, DATE_PREFIX, REPORTS_DIR, parse_partition_rel
from lake.report import ACTION, INFO

# The five states a file can name. The module docstring says what each means.
NO_EXPORT = "no_export"
NO_DELIVERY = "no_delivery"
DELIVERY = "delivery"
REFRESHING = "refreshing"
FAILED = "failed"
STATES = (NO_EXPORT, NO_DELIVERY, DELIVERY, REFRESHING, FAILED)

# The figures' own statuses, for a figure that has no number yet.
ESTIMATE = "estimate"
OK = "ok"
TOO_EARLY = "too_early"
NO_SESSIONS = "no_sessions"

# The line types that are usage. The projection scales only these, and cost per captured
# session counts only these, so a credit cannot drive either toward zero.
USAGE_LINE_TYPES = frozenset({"Usage", "DiscountedUsage", "SavingsPlanCoveredUsage"})

# The product code whose lines are split per bucket by usage type.
S3_PRODUCT = "AmazonS3"

# A ``TimedStorage-*`` usage type, with the region prefix a region other than ``us-east-1``
# puts in front of it, such as ``USE2-``. These are in GB-months, despite the name.
TIMED_STORAGE = re.compile(r"^(?:[A-Z]{2,4}[0-9]?-)?TimedStorage-")

# The nine columns the figures read, named as CUR 2.0 names them.
START = "line_item_usage_start_date"
PRODUCT = "line_item_product_code"
USAGE_TYPE = "line_item_usage_type"
RESOURCE = "line_item_resource_id"
LINE_TYPE = "line_item_line_item_type"
COST = "line_item_unblended_cost"
AMOUNT = "line_item_usage_amount"
CURRENCY = "line_item_currency_code"
ACCOUNT = "line_item_usage_account_id"
COLUMNS = (START, PRODUCT, USAGE_TYPE, RESOURCE, LINE_TYPE, COST, AMOUNT, CURRENCY, ACCOUNT)

# What an account id looks like, and what replaces it.
_ACCOUNT_ID = re.compile(r"[0-9]{12}")
ACCOUNT_MASK = "<account>"

# The places a figure is rounded to. A cost line carries up to ten places, and a summary
# printed to a dashboard needs far fewer, while a sum of floats carries noise past them.
_PLACES = 6

# The size of one read from an object's body while it is copied to the temp directory.
_CHUNK = 1 << 20

# Builds the S3 client for the export, with the checked target. A thunk, like the sweep's
# vendor source, so the instance profile's metadata call and the bucket checks run inside
# the cost step's containment, after the ping, and never at all on a host with no export.
ClientSource = Callable[[], tuple[BucketTarget, Any]]


class CostReadRefused(Exception):
    """A read this module refused for a reason of its own.

    ``reason`` is a fixed phrase that names no bucket, no key and no value, so it can go into
    the file's ``error`` and the nightly report as it stands.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def client_source(config: Config) -> ClientSource | None:
    """The thunk that connects to the export, or ``None`` when the config names none.

    The thunk parses ``cost_export`` and connects through ``bucket.connect``, so it signs
    exactly as the bucket jobs do. A value that parses as a path, rather than an ``s3://``
    target, is refused here, when the read runs, and never at config load, which runs every
    capture cycle. ``bucket.ClientFromConfig`` is not reused, because it checks
    ``backup_target``.
    """
    if config.cost_export is None:
        return None
    text = config.cost_export

    def build() -> tuple[BucketTarget, Any]:
        target = parse_backup_target(text)
        if not isinstance(target, BucketTarget):
            raise CostReadRefused("cost_export is not an s3:// location")
        from lake.bucket import connect  # lazy: only a host with an export needs it

        return connect(config, target)

    return build


# -- periods ------------------------------------------------------------------


def period_of(day: date) -> str:
    """The billing period ``YYYY-MM`` that holds ``day``."""
    return f"{day.year:04d}-{day.month:02d}"


def period_start(period: str) -> date:
    """The first day of a ``YYYY-MM`` period."""
    year, month = (int(part) for part in period.split("-"))
    return date(year, month, 1)


def days_in_period(period: str) -> int:
    """How many calendar days a ``YYYY-MM`` period has."""
    start = period_start(period)
    return month_calendar.monthrange(start.year, start.month)[1]


def previous_period(period: str) -> str:
    """The period before ``period``."""
    return period_of(period_start(period) - timedelta(days=1))


# -- the listing and the download ----------------------------------------------


def _listed(client: Any, bucket: str, prefix: str) -> list[dict[str, Any]]:
    """Every object under ``prefix``, as its key, size and ``LastModified``, in pages."""
    found: list[dict[str, Any]] = []
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token is not None:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        for item in page.get("Contents", ()):
            found.append(
                {
                    "Key": item["Key"],
                    "Size": int(item.get("Size", 0)),
                    "LastModified": item["LastModified"],
                }
            )
        if not page.get("IsTruncated"):
            return found
        token = page["NextContinuationToken"]


def _identity(listing: Iterable[Mapping[str, Any]]) -> tuple:
    """What the barrier compares between the two manifest listings."""
    return tuple(sorted((item["Key"], item["Size"], item["LastModified"]) for item in listing))


def _download(client: Any, bucket: str, key: str, path: Path) -> datetime | None:
    """Copy one object to ``path`` in chunks, and return the ``LastModified`` it answered with."""
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        with open(path, "wb") as handle:
            while True:
                chunk = body.read(_CHUNK)
                if not chunk:
                    break
                handle.write(chunk)
    finally:
        close = getattr(body, "close", None)
        if close is not None:
            close()
    return response.get("LastModified")


# -- one file, normalized ------------------------------------------------------


def _utc_day(column: pa.ChunkedArray) -> pa.ChunkedArray:
    """The UTC calendar day of each usage start, by the column's type.

    A zoned timestamp has its zone dropped first. pyarrow dates a zoned timestamp in its own
    zone, so a row at 02:00 UTC on the 1st would land on the 30th in New York's, while the
    values underneath are UTC instants, and dropping the zone keeps them. A naive timestamp is
    read as UTC. Any other type refuses the read.
    """
    kind = column.type
    if not pa.types.is_timestamp(kind):
        raise CostReadRefused("the usage start column is not a timestamp")
    if kind.tz is not None:
        column = pc.cast(column, pa.timestamp(kind.unit))
    return pc.cast(column, pa.date32())


def _text(column: pa.ChunkedArray) -> pa.ChunkedArray:
    """A text column as plain strings, with a null read as empty."""
    return pc.fill_null(pc.cast(column, pa.string()), "")


def _number(column: pa.ChunkedArray) -> pa.ChunkedArray:
    """A numeric column as doubles, with a null read as zero."""
    return pc.fill_null(pc.cast(column, pa.float64()), 0.0)


def _normalized(path: Path) -> pa.Table | None:
    """One downloaded file as the nine columns the figures read, or ``None`` when it is empty.

    The footer is read first. A zero-row file is dropped, whatever its schema, and a file
    whose footer cannot be read refuses the read.
    """
    try:
        metadata = pq.read_metadata(path)
    except Exception:  # noqa: BLE001 - any footer pyarrow cannot read is the same refusal
        metadata = None
    if metadata is None:
        raise CostReadRefused("a data file's Parquet footer could not be read")
    if metadata.num_rows == 0:
        return None
    names = set(metadata.schema.to_arrow_schema().names)
    if any(name not in names for name in COLUMNS):
        raise CostReadRefused("a data file lacks a column the figures read")
    raw = pq.read_table(path, columns=list(COLUMNS))
    return pa.table(
        {
            "day": _utc_day(raw[START]),
            "product": _text(raw[PRODUCT]),
            "usage_type": _text(raw[USAGE_TYPE]),
            "resource": _text(raw[RESOURCE]),
            "line_type": _text(raw[LINE_TYPE]),
            "cost": _number(raw[COST]),
            "amount": _number(raw[AMOUNT]),
            "currency": _text(raw[CURRENCY]),
            "account": _text(raw[ACCOUNT]),
        }
    )


@dataclass
class _Period:
    """One period's listing and rows, before any figure is computed."""

    period: str
    state: str
    delivered_at: datetime | None = None
    table: pa.Table | None = None
    accounts: set[str] = field(default_factory=set)


def _read_period(client: Any, target: BucketTarget, period: str, workdir: Path) -> _Period:
    """List, download and normalize one period, behind the manifest barrier."""
    metadata_prefix = target.key(f"metadata/BILLING_PERIOD={period}/")
    data_prefix = target.key(f"data/BILLING_PERIOD={period}/")
    before = _listed(client, target.bucket, metadata_prefix)
    data = _listed(client, target.bucket, data_prefix)
    if not before:
        # Data with no manifest is a first delivery still being written, since AWS writes the
        # manifest after the data files.
        return _Period(period, REFRESHING if data else NO_DELIVERY)
    manifest_at = max(item["LastModified"] for item in before)
    if any(item["LastModified"] > manifest_at for item in data):
        return _Period(period, REFRESHING)
    tables: list[pa.Table] = []
    for index, item in enumerate(data):
        path = workdir / f"{period}-{index:05d}.parquet"
        served_at = _download(client, target.bucket, item["Key"], path)
        if served_at is not None and served_at > manifest_at:
            return _Period(period, REFRESHING)
        table = _normalized(path)
        path.unlink()
        if table is not None:
            tables.append(table)
    after = _listed(client, target.bucket, metadata_prefix)
    if _identity(after) != _identity(before):
        return _Period(period, REFRESHING)
    if tables:
        table = pa.concat_tables(tables)
    else:
        table = _normalized_empty()
    currencies = {value for value in pc.unique(table["currency"]).to_pylist() if value}
    if len(currencies) > 1:
        raise CostReadRefused("a billing period holds more than one currency")
    if not math.isfinite(_sum(table)):
        raise CostReadRefused("a billing period's cost sums to a number that is not finite")
    if not math.isfinite(_sum(table, "amount")):
        raise CostReadRefused("a billing period's usage amount sums to a number that is not finite")
    accounts = {
        value
        for value in pc.unique(table["account"]).to_pylist()
        if value and _ACCOUNT_ID.fullmatch(value)
    }
    return _Period(period, DELIVERY, delivered_at=manifest_at, table=table, accounts=accounts)


def _normalized_empty() -> pa.Table:
    """A delivery whose every file was empty, as a table with no rows."""
    return pa.table(
        {
            "day": pa.array([], pa.date32()),
            "product": pa.array([], pa.string()),
            "usage_type": pa.array([], pa.string()),
            "resource": pa.array([], pa.string()),
            "line_type": pa.array([], pa.string()),
            "cost": pa.array([], pa.float64()),
            "amount": pa.array([], pa.float64()),
            "currency": pa.array([], pa.string()),
            "account": pa.array([], pa.string()),
        }
    )


# -- masking -------------------------------------------------------------------


def mask_resource(resource: str, accounts: Iterable[str]) -> str:
    """A resource ID with every account id it carries replaced by ``<account>``.

    An ID that starts with ``arn:`` and splits on ``:`` into six parts whose fifth is twelve
    digits gets that part replaced. Then any exact occurrence of an account id the export
    named is replaced too, which covers a bucket named after the account.
    """
    if resource.startswith("arn:"):
        parts = resource.split(":", 5)
        if len(parts) == 6 and _ACCOUNT_ID.fullmatch(parts[4]):
            parts[4] = ACCOUNT_MASK
            resource = ":".join(parts)
    for account in accounts:
        resource = resource.replace(account, ACCOUNT_MASK)
    return resource


# -- the figures ---------------------------------------------------------------


def _round(value: float) -> float:
    return round(float(value), _PLACES)


def _add(mapping: dict, key: Any, value: float) -> None:
    mapping[key] = mapping.get(key, 0.0) + value


def _rounded(value: Any) -> Any:
    """Every float in a nested mapping rounded to :data:`_PLACES`."""
    if isinstance(value, dict):
        return {key: _rounded(item) for key, item in value.items()}
    if isinstance(value, float):
        return _round(value)
    return value


def _usage_mask(table: pa.Table) -> pa.ChunkedArray:
    return pc.is_in(table["line_type"], value_set=pa.array(sorted(USAGE_LINE_TYPES)))


def _sum(table: pa.Table, column: str = "cost") -> float:
    total = pc.sum(table[column]).as_py()
    return float(total) if total is not None else 0.0


def _grouped(table: pa.Table, keys: list[str], column: str = "cost") -> list[dict[str, Any]]:
    """``Table.group_by`` summing ``column``, as rows keyed by ``keys`` and ``total``."""
    if table.num_rows == 0:
        return []
    rows = table.group_by(keys).aggregate([(column, "sum")]).to_pylist()
    return [{**{key: row[key] for key in keys}, "total": row[f"{column}_sum"]} for row in rows]


def _daily(table: pa.Table) -> dict[str, dict[str, float]]:
    """The totals per UTC day by product code."""
    daily: dict[str, dict[str, float]] = {}
    for row in _grouped(table, ["day", "product"]):
        _add(daily.setdefault(row["day"].isoformat(), {}), row["product"], row["total"])
    return daily


def _storage_daily(table: pa.Table, accounts: set[str]) -> dict[str, dict[str, dict[str, float]]]:
    """Each bucket's ``TimedStorage-*`` amount per usage type per day, in GB-months."""
    s3 = table.filter(
        pc.and_(
            pc.and_(pc.equal(table["product"], S3_PRODUCT), pc.not_equal(table["resource"], "")),
            pc.match_substring_regex(table["usage_type"], TIMED_STORAGE.pattern),
        )
    )
    series: dict[str, dict[str, dict[str, float]]] = {}
    for row in _grouped(s3, ["resource", "usage_type", "day"], column="amount"):
        bucket = mask_resource(row["resource"], accounts)
        by_type = series.setdefault(bucket, {}).setdefault(row["usage_type"], {})
        _add(by_type, row["day"].isoformat(), row["total"])
    return series


def _window(table: pa.Table, start: date, end: date) -> pa.Table:
    """The rows whose usage day falls from ``start`` through ``end``."""
    day = table["day"]
    return table.filter(
        pc.and_(
            pc.greater_equal(day, pa.scalar(start, pa.date32())),
            pc.less_equal(day, pa.scalar(end, pa.date32())),
        )
    )


def chains_days(lake_root: Path) -> set[date]:
    """Every chains session day the lake's manifest records, trimmed partitions included.

    The manifest keeps a trimmed partition's entry, which ``runway.capture_days`` does not
    see on disk, so a closed month's sessions still count after the trim.
    """
    days: set[date] = set()
    for entry in read_manifest(Path(lake_root)):
        partition = entry.get("partition") if isinstance(entry, dict) else None
        if not isinstance(partition, str):
            continue
        ref = parse_partition_rel(partition)
        if ref is not None and ref.surface == CHAINS:
            days.add(ref.day)
    return days


def _per_session(usage: float, sessions: int, through: date) -> dict[str, Any]:
    """Cost per captured session, or the reason it has no number."""
    if sessions == 0:
        return {
            "status": NO_SESSIONS,
            "sessions": 0,
            "usage_cost": usage,
            "through": through.isoformat(),
        }
    return {
        "status": OK,
        "sessions": sessions,
        "usage_cost": usage,
        "value": usage / sessions,
        "through": through.isoformat(),
    }


def current_figures(read: _Period, *, sessions: set[date], accounts: set[str]) -> dict[str, Any]:
    """The ``current`` block for a period with a delivery."""
    assert read.table is not None and read.delivered_at is not None
    table = read.table
    period = read.period
    first = period_start(period)
    month_days = days_in_period(period)
    usage = _usage_mask(table)
    # N is the newest usage day. A tax, credit or fee line can be dated the period's last day
    # while usage runs only to the 15th, so a day taken over every line would move N, and the
    # projection and cost per session with it. A period with no usage line has no N, which
    # reads as too early.
    usage_days = table.filter(usage)["day"]
    newest = pc.max(usage_days).as_py() if len(usage_days) else None
    complete = newest - timedelta(days=1) if newest is not None else None
    too_early = complete is None or complete < first

    service: dict[str, dict[str, float]] = {}
    for row in _grouped(table.append_column("is_usage", usage), ["product", "is_usage"]):
        split = service.setdefault(row["product"], {"usage": 0.0, "other": 0.0})
        _add(split, "usage" if row["is_usage"] else "other", row["total"])
    usage_type: dict[str, dict[str, float]] = {}
    for row in _grouped(table, ["product", "usage_type"]):
        _add(usage_type.setdefault(row["product"], {}), row["usage_type"], row["total"])
    with_resource = table.filter(pc.not_equal(table["resource"], ""))
    resource: dict[str, float] = {}
    for row in _grouped(with_resource, ["resource"]):
        _add(resource, mask_resource(row["resource"], accounts), row["total"])
    s3_rows = with_resource.filter(pc.equal(with_resource["product"], S3_PRODUCT))
    s3: dict[str, dict[str, float]] = {}
    for row in _grouped(s3_rows, ["resource", "usage_type"]):
        bucket = mask_resource(row["resource"], accounts)
        _add(s3.setdefault(bucket, {}), row["usage_type"], row["total"])

    other = _sum(table.filter(pc.invert(usage)))
    if too_early:
        projection: dict[str, Any] = {"status": TOO_EARLY}
        per_session: dict[str, Any] = {"status": TOO_EARLY}
        window_sessions = None
    else:
        assert complete is not None
        window = _window(table.filter(usage), first, complete)
        window_usage = _sum(window)
        window_days = (complete - first).days + 1
        projection = {
            "status": ESTIMATE,
            "month_end": window_usage / window_days * month_days + other,
            "usage_through": window_usage,
            "other": other,
            "window_days": window_days,
            "through": complete.isoformat(),
        }
        window_sessions = sum(1 for day in sessions if first <= day <= complete)
        per_session = _per_session(window_usage, window_sessions, complete)

    return _rounded(
        {
            "period": period,
            "delivered_at": _instant(read.delivered_at),
            "days_in_month": month_days,
            "newest_day": newest.isoformat() if newest is not None else None,
            "complete_through": None if too_early or complete is None else complete.isoformat(),
            "total": _sum(table),
            "mtd": {
                "service": service,
                "usage_type": usage_type,
                "resource": resource,
                "s3": s3,
            },
            "daily": _daily(table),
            "storage_daily": _storage_daily(table, accounts),
            "sessions": window_sessions,
            "cost_per_session": per_session,
            "projection": projection,
        }
    )


def previous_figures(read: _Period, *, sessions: set[date], accounts: set[str]) -> dict[str, Any]:
    """The ``previous`` block for a closed period with a delivery, over all of its days."""
    assert read.table is not None and read.delivered_at is not None
    table = read.table
    first = period_start(read.period)
    month_days = days_in_period(read.period)
    last = first + timedelta(days=month_days - 1)
    usage = _sum(table.filter(_usage_mask(table)))
    count = sum(1 for day in sessions if first <= day <= last)
    return _rounded(
        {
            "period": read.period,
            "delivered_at": _instant(read.delivered_at),
            "days_in_month": month_days,
            "total": _sum(table),
            "daily": _daily(table),
            "storage_daily": _storage_daily(table, accounts),
            "sessions": count,
            "cost_per_session": _per_session(usage, count, last),
        }
    )


def _instant(value: datetime) -> str:
    """An S3 ``LastModified`` as a UTC ISO instant."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


# -- the read ------------------------------------------------------------------


def _currency(*reads: _Period) -> str | None:
    for read in reads:
        if read.table is not None:
            values = [value for value in pc.unique(read.table["currency"]).to_pylist() if value]
            if values:
                return values[0]
    return None


def read_cost(source: ClientSource, lake_root: Path | str, *, now: datetime) -> dict[str, Any]:
    """Read the export for the period holding ``now`` and the one before, as a report entry.

    Raises on any failure. :func:`run` turns that into a ``failed`` entry. The temp
    directory is outside the lake and is removed on the way out, an exception included.
    """
    period = period_of(now.astimezone(UTC).date())
    entry = _entry(now, period)
    target, client = source()
    with tempfile.TemporaryDirectory(prefix="marketlake-cost-") as scratch:
        workdir = Path(scratch)
        current = _read_period(client, target, period, workdir)
        if current.state == NO_DELIVERY:
            any_period = target.key("data/BILLING_PERIOD=")
            page = client.list_objects_v2(Bucket=target.bucket, Prefix=any_period, MaxKeys=1)
            if not page.get("Contents"):
                entry["state"] = NO_EXPORT
                return entry
        # The previous period fails apart from the current one. AWS re-delivers a closed
        # month for up to two weeks, so a corrupt chunk there would otherwise blank this
        # month's figures for as long.
        previous_name = previous_period(period)
        previous = _Period(previous_name, NO_DELIVERY)
        try:
            previous = _read_period(client, target, previous_name, workdir)
        except Exception as exc:  # noqa: BLE001 - the closed month never fails the current one
            entry["previous_error"] = _error_of(exc)
    entry["state"] = current.state
    if current.state != DELIVERY and previous.state != DELIVERY:
        return entry
    accounts = current.accounts | previous.accounts
    sessions = chains_days(Path(lake_root))
    entry["currency"] = _currency(current, previous)
    if current.state == DELIVERY:
        entry["current"] = current_figures(current, sessions=sessions, accounts=accounts)
    if previous.state == DELIVERY:
        try:
            entry["previous"] = previous_figures(previous, sessions=sessions, accounts=accounts)
        except Exception as exc:  # noqa: BLE001 - the closed month never fails the current one
            entry["previous_error"] = _error_of(exc)
    return entry


def _error_of(exc: BaseException) -> str:
    """What a file says of a failure: a refusal's fixed reason, or the exception's type."""
    return exc.reason if isinstance(exc, CostReadRefused) else type(exc).__name__


def _entry(now: datetime, period: str) -> dict[str, Any]:
    eastern = now.astimezone(MARKET_TZ)
    return {
        "at": eastern.isoformat(),
        "day": eastern.date().isoformat(),
        "period": period,
        "state": FAILED,
        "error": None,
        "currency": None,
    }


def run(source: ClientSource, lake_root: Path | str, *, now: datetime) -> dict[str, Any]:
    """One night's read as a report entry, with any failure turned into a ``failed`` one.

    The entry carries the exception's type, never its message, because the sweep's digest
    reaches ntfy.sh and a message can quote a bucket name. A refusal of this module's own
    carries its fixed reason instead.
    """
    try:
        return read_cost(source, lake_root, now=now)
    except Exception as exc:  # noqa: BLE001 - every failure is a failed file, never a crash
        error = _error_of(exc)
    entry = _entry(now, period_of(now.astimezone(UTC).date()))
    entry["error"] = error
    return entry


def report_line(entry: Mapping[str, Any]) -> tuple[str, str] | None:
    """The nightly report's line for an entry and its kind, or ``None`` for a delivery.

    ``failed`` is ``ACTION``. The other three are ``INFO``, because ``no_export`` is expected
    on every night before the first delivery and a refresh settles itself by the next one.
    Each line carries one ``": "``, so ``report.redacted`` keeps it whole.
    """
    state = entry.get("state")
    period = entry.get("period")
    if state == DELIVERY:
        return None
    if state == NO_EXPORT:
        return "aws cost read: no export has been delivered yet", INFO
    if state == NO_DELIVERY:
        return f"aws cost read: no delivery yet for {period}", INFO
    if state == REFRESHING:
        return f"aws cost read: the {period} export was refreshing, read again next night", INFO
    return f"aws cost read failed: {entry.get('error')}", ACTION


# -- the report file -----------------------------------------------------------


def cost_report_dir(lake_root: Path | str, day: date) -> Path:
    """Where one night's cost files land: ``reports/aws_cost/date=D/``."""
    return Path(lake_root) / REPORTS_DIR / AWS_COST_DIR / f"{DATE_PREFIX}{day.isoformat()}"


def write_cost_report(
    lake_root: Path | str,
    entry: Mapping[str, Any],
    *,
    now: datetime,
    pid: int | None = None,
) -> Path:
    """File one night's cost entry, and hand back the path it landed at.

    Write-once under ``report.py``'s rule 3: the name is the Eastern stamp to the microsecond
    and the pid, opened in mode ``"x"``. A fixed name would refuse a second sweep on one
    night, and the nightly upload compares by size, so a same-size rewrite would never reach
    the backup. A missing lake root is refused, as ``report.write_nightly`` refuses it,
    because ``parents=True`` would otherwise create the lake itself. A number that is not
    finite raises ``ValueError`` before the file is created.
    """
    pid = os.getpid() if pid is None else pid
    eastern = now.astimezone(MARKET_TZ)
    root = Path(lake_root)
    if not root.is_dir():
        raise FileNotFoundError(f"lake root missing: {root}")
    directory = cost_report_dir(root, eastern.date())
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{eastern.strftime('%H%M%S%f')}-{pid}.json"
    # Encoded before the file opens, with ``allow_nan`` off, so a number JSON cannot hold
    # raises here and leaves no torn file behind, rather than writing a bare ``NaN`` the
    # page's ``JSON.parse`` refuses.
    text = json.dumps(entry, sort_keys=True, allow_nan=False)
    with open(path, "x", encoding="utf-8") as handle:
        handle.write(text)
        handle.write("\n")
    return path


__all__ = [
    "ACCOUNT_MASK",
    "COLUMNS",
    "DELIVERY",
    "FAILED",
    "NO_DELIVERY",
    "NO_EXPORT",
    "REFRESHING",
    "STATES",
    "USAGE_LINE_TYPES",
    "ClientSource",
    "CostReadRefused",
    "chains_days",
    "client_source",
    "cost_report_dir",
    "days_in_period",
    "mask_resource",
    "period_of",
    "previous_period",
    "read_cost",
    "report_line",
    "run",
    "write_cost_report",
]
