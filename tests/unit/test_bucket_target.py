"""The two forms of ``backup_target``, and the keys the bucket form needs.

A filesystem path stays the default and loads exactly as before. An ``s3://`` URL loads
as a ``BucketTarget``, and the scheme is read before any ``Path`` exists, because
``Path("s3://bucket/x")`` collapses to ``s3:/bucket/x``, a local directory. The bucket's
access key, secret key and region are required only when the target is a bucket, and the
two keys load as ``Secret`` values that join every page's secret list.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lake.config import BucketTarget, Config, ConfigError, Secret, parse_backup_target

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


@pytest.mark.parametrize(
    "text", ["s3://", "s3://UPPER", "s3://a", "s3://bad_name", "s3://x..y", "s3://ok-name/../up"]
)
def test_a_malformed_bucket_url_is_refused(text):
    with pytest.raises(ConfigError, match="backup_target"):
        parse_backup_target(text)


@pytest.mark.parametrize("text", ["gs://lake-backup", "file:///Volumes/ssd", "S3://lake-backup"])
def test_any_other_scheme_is_refused_rather_than_read_as_a_path(text):
    with pytest.raises(ConfigError, match="scheme"):
        parse_backup_target(text)


def test_a_bucket_target_needs_its_three_keys():
    with pytest.raises(ConfigError) as refused:
        Config.from_mapping({**BASE, "backup_target": "s3://lake-backup"})
    for key in KEYS:
        assert key in str(refused.value)


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
