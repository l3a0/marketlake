"""The two forms of ``backup_target``, and the keys the bucket form needs.

A filesystem path stays the default and loads exactly as before. An ``s3://`` URL loads
as a ``BucketTarget``, and the scheme is read before any ``Path`` exists, because
``Path("s3://bucket/x")`` collapses to ``s3:/bucket/x``, a local directory. The two keys
load as ``Secret`` values that join every page's secret list.

Loading refuses no backup setting. Capture loads the config every minute, so a refusal
there would stop capture over a value only the backup reads. The strict checks live in
``require_bucket_settings``, which each bucket job calls when it runs: a valid bucket
name and prefix, all three bucket keys, and a region shaped like an AWS region name.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import botocore
import pytest

import lake
from lake.config import (
    BucketTarget,
    Config,
    ConfigError,
    Secret,
    parse_backup_target,
    require_bucket_settings,
)

BASE = {
    "lake_root": "/data/lake",
    "backup_target": "/Volumes/ssd/lake",
    "healthchecks_ping_key": "PING-KEY-SECRET",
    "ntfy_topic": "topic-secret-xyz",
    "schwab_api_key": "SCHWAB-API-KEY-SECRET",
    "schwab_app_secret": "SCHWAB-APP-SECRET-VALUE",
}
KEYS = {
    "bucket_access_key_id": "AKIDBUCKETKEY",
    "bucket_secret_access_key": "bucket-secret-value",
    "bucket_region": "us-east-2",
}


def test_a_path_stays_a_path():
    cfg = Config.from_mapping(BASE)
    assert cfg.backup_target == Path("/Volumes/ssd/lake")
    assert cfg.bucket_access_key_id is None and cfg.bucket_secret_access_key is None


def test_a_tilde_path_still_expands(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    assert parse_backup_target("~/ssd") == tmp_path / "ssd"


@pytest.mark.parametrize(
    ("text", "bucket", "prefix", "shown"),
    [
        ("s3://lake-backup", "lake-backup", "", "s3://lake-backup"),
        ("s3://lake-backup/", "lake-backup", "", "s3://lake-backup"),
        ("s3://lake-backup/lake", "lake-backup", "lake", "s3://lake-backup/lake"),
        ("s3://lake-backup//a/b/", "lake-backup", "a/b", "s3://lake-backup/a/b"),
    ],
)
def test_an_s3_url_is_a_bucket_target(text, bucket, prefix, shown):
    target = parse_backup_target(text)
    assert target == BucketTarget(bucket=bucket, prefix=prefix)
    assert str(target) == shown


def test_a_bucket_target_maps_keys_both_ways():
    target = BucketTarget("lake-backup", "lake")
    assert target.key("chains/x.parquet") == "lake/chains/x.parquet"
    assert target.rel("lake/chains/x.parquet") == "chains/x.parquet"
    assert target.rel("lakeside/x") is None
    bare = BucketTarget("lake-backup")
    assert bare.key("manifest.jsonl") == "manifest.jsonl"
    assert bare.list_prefix == ""


MALFORMED = ["s3://", "s3://UPPER", "s3://a", "s3://bad_name", "s3://x..y", "s3://ok-name/../up"]


@pytest.mark.parametrize("text", MALFORMED)
def test_a_malformed_bucket_url_loads_and_is_refused_when_a_job_runs(text):
    cfg = Config.from_mapping({**BASE, "backup_target": text, **KEYS})
    assert isinstance(cfg.backup_target, BucketTarget)
    with pytest.raises(ConfigError, match="bucket|prefix"):
        require_bucket_settings(cfg)


@pytest.mark.parametrize("text", ["s3://lake-backup", "s3://lake-backup/a/b", "s3://a.b-c/x"])
def test_a_well_formed_bucket_with_its_keys_passes_the_job_checks(text):
    cfg = Config.from_mapping({**BASE, "backup_target": text, **KEYS})
    assert require_bucket_settings(cfg) == cfg.backup_target


@pytest.mark.parametrize(
    "text", ["gs://lake-backup", "file:///Volumes/ssd", "S3://lake-backup", "smb://nas/share"]
)
def test_any_other_scheme_loads_as_a_path_exactly_as_before(text):
    # Before the bucket form existed every value was ``Path(text).expanduser()``, and a
    # path the backup cannot reach fails the backup rather than the load.
    cfg = Config.from_mapping({**BASE, "backup_target": text})
    assert cfg.backup_target == Path(text).expanduser()
    assert str(Config.from_mapping({**BASE, "backup_target": "smb://nas/share"}).backup_target) == (
        "smb:/nas/share"
    )


def test_a_path_value_is_not_stripped():
    # The path form keeps the exact text it always took, surrounding spaces included.
    assert parse_backup_target(" /Volumes/ssd ") == Path(" /Volumes/ssd ")


def test_a_bucket_target_loads_without_its_keys_and_a_job_names_all_three():
    cfg = Config.from_mapping({**BASE, "backup_target": "s3://lake-backup"})
    assert cfg.backup_target == BucketTarget("lake-backup")
    with pytest.raises(ConfigError) as refused:
        require_bucket_settings(cfg)
    for key in KEYS:
        assert key in str(refused.value)
    assert "\n" not in str(refused.value)


def test_the_job_checks_refuse_a_path_target_unless_a_bucket_is_named():
    cfg = Config.from_mapping({**BASE, **KEYS})
    with pytest.raises(ConfigError, match="not an s3:// bucket"):
        require_bucket_settings(cfg)
    assert require_bucket_settings(cfg, BucketTarget("lake-backup")) == BucketTarget("lake-backup")


def _aws_regions() -> list[str]:
    """Every region botocore's own partition data names, the pseudo-regions aside."""
    data = json.loads((Path(botocore.__file__).parent / "data" / "partitions.json").read_text())
    names = {region for partition in data["partitions"] for region in partition["regions"]}
    return sorted(name for name in names if not name.endswith("-global"))


def test_the_partition_data_holds_the_awkward_region_shapes():
    regions = _aws_regions()
    for shape in ("us-gov-west-1", "us-isob-east-1", "cn-northwest-1", "eusc-de-east-1"):
        assert shape in regions


@pytest.mark.parametrize("region", _aws_regions())
def test_every_aws_region_name_passes_the_region_check(region):
    cfg = Config.from_mapping(
        {**BASE, "backup_target": "s3://lake-backup", **KEYS, "bucket_region": region}
    )
    require_bucket_settings(cfg)


@pytest.mark.parametrize(
    "region", ["us east 2", "us-east-2/", "US-EAST-2", "us-east", "useast2", "us-east-"]
)
def test_a_malformed_region_loads_and_is_refused_when_a_job_runs(region):
    cfg = Config.from_mapping(
        {**BASE, "backup_target": "s3://lake-backup", **KEYS, "bucket_region": region}
    )
    with pytest.raises(ConfigError, match="bucket_region"):
        require_bucket_settings(cfg)


def test_a_bucket_target_with_its_keys_loads_them_as_secrets():
    cfg = Config.from_mapping({**BASE, "backup_target": "s3://lake-backup/lake", **KEYS})
    assert cfg.backup_target == BucketTarget("lake-backup", "lake")
    assert type(cfg.bucket_access_key_id) is Secret
    assert type(cfg.bucket_secret_access_key) is Secret
    assert cfg.bucket_secret_access_key.reveal() == "bucket-secret-value"
    assert cfg.bucket_region == "us-east-2"
    shown = repr(cfg)
    assert "bucket-secret-value" not in shown
    assert "AKIDBUCKETKEY" not in shown


def test_the_keys_may_sit_beside_a_path_target():
    # Setup step 4 puts the key in the file before the target is switched, so the first
    # upload can run while compaction still copies to the path.
    cfg = Config.from_mapping({**BASE, **KEYS})
    assert cfg.backup_target == Path("/Volumes/ssd/lake")
    assert cfg.bucket_secret_access_key == Secret("bucket-secret-value")


def test_the_page_secrets_hold_the_bucket_keys_when_present():
    without = Config.from_mapping(BASE).page_secrets()
    assert without == ("PING-KEY-SECRET", "topic-secret-xyz")
    with_keys = Config.from_mapping({**BASE, **KEYS}).page_secrets()
    assert with_keys == (
        "PING-KEY-SECRET",
        "topic-secret-xyz",
        "AKIDBUCKETKEY",
        "bucket-secret-value",
    )


def _publisher_sites() -> list[tuple[str, int, ast.Call]]:
    """Every ``Publisher(...)`` construction under ``src/lake``, as (file, line, call)."""
    source = Path(lake.__file__).parent
    sites = []
    for path in sorted(source.glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
            if name == "Publisher":
                sites.append((path.name, node.lineno, node))
    return sites


def test_every_publisher_takes_its_secrets_from_page_secrets():
    # ``page_secrets`` is what adds the bucket's two key values. A construction site that
    # spelled its own tuple would let a page carry the bucket key to ntfy, and only the
    # compaction site has a behavioral test of its own.
    sites = _publisher_sites()
    files = {name for name, _, _ in sites}
    for expected in (
        "alert.py",
        "compact.py",
        "control_plane.py",
        "daemon.py",
        "probe_calendar.py",
        "sweep.py",
    ):
        assert expected in files
    assert sum(name == "control_plane.py" for name, _, _ in sites) == 2
    for name, line, call in sites:
        secrets = [kw.value for kw in call.keywords if kw.arg == "secrets"]
        assert len(secrets) == 1, f"{name}:{line} passes no secrets"
        value = secrets[0]
        assert (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Attribute)
            and value.func.attr == "page_secrets"
            and isinstance(value.func.value, ast.Name)
            and value.func.value.id == "config"
            and not value.args
        ), f"{name}:{line} builds its own secrets instead of config.page_secrets()"
