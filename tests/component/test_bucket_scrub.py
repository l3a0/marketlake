"""The Sunday scrub of a bucket target, against the fake S3 client.

``bucket.bucket_scrub`` gives the path scrub's findings and its ping rules. The cases
follow the issue's list, marketlake #639.

1. The prefix check, which also names where a copy diverged.
2. The forward pass, by ``HeadObject`` in checksum mode, inside the watermark.
3. The reverse pass, by listing, skipping ``SCRUB_EXCLUSIONS``.
4. A bucket that refuses or cannot be reached, each under its own name, never raising.
5. The versioning line, which withholds nothing.

Then the Sunday job itself, with a bucket target, from ``sunday_maintenance`` up to
``control_plane.main``.
"""

from __future__ import annotations

import base64
import hashlib
import json
from datetime import date, datetime
from pathlib import Path

import pytest

from lake import bucket
from lake import control_plane as cp
from lake.bucket import bucket_scrub, first_upload
from lake.calendar import MARKET_TZ
from lake.config import BucketTarget
from lake.manifest import append_manifest, manifest_path, sha256_file
from tests.support.bucket import FakeS3, client_error, unreachable
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.lake import FixtureLake
from tests.support.pinger import FakePinger

DAY = date(2026, 8, 28)
TARGET = BucketTarget(bucket="lake-backup", prefix="lake")
PARTITION = "chains/ticker=SPY/date=2026-08-28.parquet"
CALENDAR = weekday_sessions(date(2026, 8, 31), date(2026, 9, 7))
SUNDAY_20 = et(2026, 8, 30, 20, 0)
FRESH_MINT = et(2026, 8, 30, 19, 30)
URL = "https://hc-ping.com/secret-key/sunday"


def _key(rel: str) -> str:
    return TARGET.key(rel)


def _uploaded(root: Path) -> tuple[Path, FakeS3]:
    lake = FixtureLake(root).with_chains("SPY", DAY).with_quotes("SPY", DAY).build()
    client = FakeS3()
    first_upload(
        lake,
        TARGET,
        client=client,
        clock=ManualClock(datetime(2026, 8, 28, 19, 0, tzinfo=MARKET_TZ)),
        calendar=CALENDAR,
    )
    return lake, client


def test_a_clean_bucket_scrubs_clean(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    result = bucket_scrub(lake, TARGET, client)
    assert result.ok
    assert result.notes == ()
    assert result.target == "s3://lake-backup/lake"


# -- 1. the prefix check ---------------------------------------------------------


def test_a_bucket_with_no_manifest_copy_withholds(tmp_path):
    lake = FixtureLake(tmp_path / "lake").with_chains("SPY", DAY).build()
    result = bucket_scrub(lake, TARGET, FakeS3())
    assert result.manifest_missing and not result.ok


def test_a_diverged_copy_names_the_byte(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    raw = manifest_path(lake).read_bytes()
    client.store(_key("manifest.jsonl"), raw[:10] + b"#" + raw[11:])
    result = bucket_scrub(lake, TARGET, client)
    assert result.manifest_diverged_at == 10
    assert "diverged" in result.problem


def test_a_copy_behind_the_lake_reads_as_pending_not_loss(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    extra = lake / "chains" / "ticker=QQQ" / "date=2026-08-28.parquet"
    extra.parent.mkdir(parents=True)
    extra.write_bytes(b"later")
    append_manifest(
        lake,
        partition="chains/ticker=QQQ/date=2026-08-28.parquet",
        source="compaction",
        sha256=sha256_file(extra),
        rows=1,
        fetched_at=None,
    )
    result = bucket_scrub(lake, TARGET, client)
    assert result.ok
    assert result.pending == ("chains/ticker=QQQ/date=2026-08-28.parquet",)


# -- 2. the forward pass ---------------------------------------------------------


def test_an_overwritten_object_is_a_mismatch(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.store(_key(PARTITION), b"overwritten")
    result = bucket_scrub(lake, TARGET, client)
    assert result.sha_mismatches == (PARTITION,)
    assert not result.ok


def test_an_object_with_no_stored_checksum_is_a_mismatch(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.store(_key(PARTITION), (lake / PARTITION).read_bytes(), checksum=None)
    assert bucket_scrub(lake, TARGET, client).sha_mismatches == (PARTITION,)


def test_a_composite_checksum_is_a_mismatch(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    digest = base64.b64encode(hashlib.sha256((lake / PARTITION).read_bytes()).digest()).decode()
    client.store(
        _key(PARTITION),
        (lake / PARTITION).read_bytes(),
        checksum=f"{digest}-3",
        checksum_type="COMPOSITE",
    )
    assert bucket_scrub(lake, TARGET, client).sha_mismatches == (PARTITION,)


def test_a_full_object_digest_with_a_composite_type_is_a_mismatch(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    digest = base64.b64encode(hashlib.sha256((lake / PARTITION).read_bytes()).digest()).decode()
    client.store(
        _key(PARTITION), (lake / PARTITION).read_bytes(), checksum=digest, checksum_type="COMPOSITE"
    )
    assert bucket_scrub(lake, TARGET, client).sha_mismatches == (PARTITION,)


def test_a_deleted_object_is_missing(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    del client.objects[_key(PARTITION)]
    result = bucket_scrub(lake, TARGET, client)
    assert result.missing == (PARTITION,)
    assert not result.ok


# -- 3. the reverse pass ---------------------------------------------------------


def test_an_object_the_lake_never_recorded_is_an_orphan_that_does_not_withhold(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.store(_key("chains/stray.parquet"), b"stray")
    result = bucket_scrub(lake, TARGET, client)
    assert result.orphans == ("chains/stray.parquet",)
    assert result.ok


def test_an_object_recorded_past_the_watermark_is_unaccounted(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    rel = "chains/ticker=QQQ/date=2026-08-28.parquet"
    (lake / rel).parent.mkdir(parents=True)
    (lake / rel).write_bytes(b"later")
    append_manifest(
        lake,
        partition=rel,
        source="compaction",
        sha256=sha256_file(lake / rel),
        rows=1,
        fetched_at=None,
    )
    client.store(_key(rel), b"later")
    result = bucket_scrub(lake, TARGET, client)
    assert result.unaccounted == (rel,)
    assert not result.ok


def test_reports_and_the_journal_are_skipped_by_the_reverse_pass(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.store(_key("reports/date=2026-08-28.md"), b"r")
    client.store(_key("journal/metadata.json"), b"{}")
    assert bucket_scrub(lake, TARGET, client).orphans == ()


def test_objects_outside_the_prefix_are_not_the_lakes(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.store("live-check-20261005T000000Z/probe", b"probe")
    assert bucket_scrub(lake, TARGET, client).orphans == ()


def test_a_listing_across_several_pages_is_read_whole(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.page_size = 1
    client.store(_key("chains/stray.parquet"), b"stray")
    assert bucket_scrub(lake, TARGET, client).orphans == ("chains/stray.parquet",)


# -- 4. a bucket that refuses or cannot be reached --------------------------------


@pytest.mark.parametrize("code", ["AccessDenied", "InvalidAccessKeyId", "403"])
def test_a_refused_credential_is_named_refused_and_never_raises(tmp_path, code):
    lake, client = _uploaded(tmp_path / "lake")
    client.fail_with = client_error(code, "HeadObject", 403)
    result = bucket_scrub(lake, TARGET, client)
    assert result.bucket_refused == code
    assert result.bucket_unreachable is None
    assert "refused" in result.problem and "access key" in result.problem


def test_a_failed_connection_is_named_unreachable_and_never_raises(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.fail_with = unreachable()
    result = bucket_scrub(lake, TARGET, client)
    assert result.bucket_unreachable == "EndpointConnectionError"
    assert result.bucket_refused is None
    assert "could not be reached" in result.problem


@pytest.mark.parametrize(
    ("code", "status"),
    [
        ("SignatureDoesNotMatch", 403),
        ("ExpiredToken", 400),
        ("AccountProblem", 403),
    ],
)
def test_every_credential_code_is_named_refused(tmp_path, code, status):
    lake, client = _uploaded(tmp_path / "lake")
    client.fail_with = client_error(code, "HeadObject", status)
    result = bucket_scrub(lake, TARGET, client)
    assert result.bucket_refused == code


@pytest.mark.parametrize(
    ("code", "status"),
    [
        ("SlowDown", 503),
        ("ServiceUnavailable", 503),
        ("InternalError", 500),
        ("TooManyRequests", 429),
        ("Throttling", 400),
        ("RequestTimeout", 400),
        ("SomethingNew", 502),
    ],
)
def test_a_busy_or_failing_service_is_named_unavailable_not_refused(tmp_path, code, status):
    # A 503 SlowDown is S3 asking for fewer requests, which no new key repairs.
    lake, client = _uploaded(tmp_path / "lake")
    client.fail_with = client_error(code, "HeadObject", status)
    result = bucket_scrub(lake, TARGET, client)
    assert result.bucket_unreachable == code
    assert result.bucket_refused is None
    assert "access key" not in result.problem
    assert "unavailable" in result.problem


def test_any_other_answer_is_named_failed_with_its_code(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.fail_with = client_error("NoSuchBucket", "HeadObject", 404)
    result = bucket_scrub(lake, TARGET, client)
    assert result.bucket_failed == "NoSuchBucket"
    assert result.bucket_refused is None and result.bucket_unreachable is None
    assert "access key" not in result.problem
    assert "NoSuchBucket" in result.problem


def test_the_versioning_line_survives_a_scrub_that_stops_early(tmp_path):
    # The bucket answers, so its versioning status is known, even though the scrub
    # stops at the missing manifest before the forward pass.
    lake, client = _uploaded(tmp_path / "lake")
    client.versioning = "Suspended"
    del client.objects[_key("manifest.jsonl")]
    result = bucket_scrub(lake, TARGET, client)
    assert result.manifest_missing
    assert any(line.startswith("bucket versioning is Suspended") for line in result.notes)


class _HeadsFail(FakeS3):
    """A bucket whose versioning answers and whose object requests then fail."""

    def head_object(self, **kwargs):
        raise unreachable()


def test_the_versioning_line_survives_a_bucket_that_fails_mid_scrub(tmp_path):
    lake, uploaded = _uploaded(tmp_path / "lake")
    client = _HeadsFail(versioning="Suspended")
    client.objects = uploaded.objects
    result = bucket_scrub(lake, TARGET, client)
    assert result.bucket_unreachable == "EndpointConnectionError"
    assert any(line.startswith("bucket versioning is Suspended") for line in result.notes)


def test_a_bug_still_raises(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.fail_with = ZeroDivisionError("a real bug")
    with pytest.raises(ZeroDivisionError):
        bucket_scrub(lake, TARGET, client)


# -- 5. versioning ---------------------------------------------------------------


@pytest.mark.parametrize("status", ["Suspended", None])
def test_versioning_that_is_not_enabled_is_a_report_line_that_withholds_nothing(tmp_path, status):
    lake, client = _uploaded(tmp_path / "lake")
    client.versioning = status
    result = bucket_scrub(lake, TARGET, client)
    assert result.ok
    assert any(line.startswith("bucket versioning is") for line in result.notes)


# -- the Sunday job --------------------------------------------------------------


class _Pushes:
    """A transport recording each push. The real one POSTs to ntfy."""

    def __init__(self) -> None:
        self.sent = []

    def send(self, message) -> None:
        self.sent.append(message)


def _sunday(lake: Path, client: FakeS3):
    pinger = FakePinger()
    outcome = cp.sunday_maintenance(
        lake_root=lake,
        backup_target=TARGET,
        bucket_client=client,
        now=SUNDAY_20,
        calendar=CALENDAR,
        schedule_reader=lambda: "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n",
        pinger=pinger,
        ping_url=URL,
        mint=FRESH_MINT,
        canary=lambda: True,
    )
    return outcome, pinger


def test_the_sunday_job_pings_on_a_clean_bucket(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    outcome, pinger = _sunday(lake, client)
    assert outcome.problems == ()
    assert pinger.urls == [URL]


def test_the_sunday_job_withholds_the_ping_for_an_unreachable_bucket_and_runs_the_rest(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.fail_with = unreachable()
    outcome, pinger = _sunday(lake, client)
    assert pinger.urls == []
    assert any("could not be reached" in problem for problem in outcome.problems)
    # The canary and the coverage assertion still ran.
    assert outcome.canary_passed and outcome.covered is True


def test_the_sunday_job_reports_suspended_versioning_and_still_pings(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.versioning = "Suspended"
    outcome, pinger = _sunday(lake, client)
    assert pinger.urls == [URL]
    assert any("versioning is Suspended" in line for line in outcome.report)


def test_a_bucket_target_without_a_client_is_a_caller_error(tmp_path):
    lake, _ = _uploaded(tmp_path / "lake")
    with pytest.raises(ValueError, match="bucket_client"):
        _sunday(lake, None)


_KEYS = (
    "bucket_access_key_id: AKIDCONFIG\n"
    "bucket_secret_access_key: secret-bucket-key\n"
    "bucket_region: us-east-2\n"
)


def _sunday_cli(tmp_path, monkeypatch, lake: Path, *, target=str(TARGET), keys=_KEYS):
    """Run ``control_plane.main sunday`` over a config naming ``target``.

    Every seam that reaches past the process is replaced. The canary records that it
    ran, so a test can show the job carried on past a bucket finding.
    """
    config = write_config(tmp_path, lake)
    text = config.read_text().replace(
        f"backup_target: {tmp_path / 'ssd'}", f"backup_target: {target}"
    )
    config.write_text(text + keys)
    pinger = FakePinger()
    canaries: list[str] = []

    def canary(**kwargs):
        def run():
            canaries.append("ran")
            return True

        return run

    monkeypatch.setattr(cp, "read_pmset_schedule", lambda: "")
    monkeypatch.setattr(cp, "UrllibPinger", lambda: pinger)
    monkeypatch.setattr(cp, "token_canary", canary)
    monkeypatch.setattr(cp, "NtfyTransport", lambda topic: _Pushes())
    monkeypatch.setattr(cp, "read_exclusions", lambda targets: "")
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
    token = tmp_path / "token.json"
    token.write_text(json.dumps({"creation_timestamp": FRESH_MINT.timestamp(), "token": {}}))

    code = cp.main(
        ["sunday", "--config", str(config), "--token", str(token)],
        clock=ManualClock(start=SUNDAY_20),
        calendar=CALENDAR,
    )
    return code, pinger, canaries


def test_the_sunday_cli_scrubs_the_configured_bucket(tmp_path, capsys, monkeypatch):
    """``control_plane.main`` builds the client from the config and scrubs the bucket.

    The overwritten object is in the bucket alone, so only a scrub that reached the
    bucket can name it. A wiring slip that scrubbed a path instead would fail
    differently.
    """
    lake, client = _uploaded(tmp_path / "lake")
    client.store(_key(PARTITION), b"overwritten")
    built = []

    def fake_client(cfg):
        built.append(cfg.bucket_secret_access_key.reveal())
        return client

    monkeypatch.setattr(bucket, "client_from_config", fake_client)

    code, pinger, _ = _sunday_cli(tmp_path, monkeypatch, lake)

    assert code == 1
    assert built == ["secret-bucket-key"]
    assert pinger.urls == []
    printed = capsys.readouterr().out
    assert f"backup file does not match the lake: {PARTITION}" in printed
    assert "s3://lake-backup/lake" in printed
    assert "secret-bucket-key" not in printed


@pytest.mark.parametrize(
    ("target", "keys", "named"),
    [
        (str(TARGET), "", "bucket_access_key_id"),
        ("s3://Legacy_Bucket/lake", _KEYS, "names no valid bucket"),
        (str(TARGET), _KEYS.replace("us-east-2", "us east 2"), "bucket_region"),
    ],
)
def test_unusable_bucket_settings_are_a_sunday_finding_and_the_job_runs_on(
    tmp_path, capsys, monkeypatch, target, keys, named
):
    # A traceback here would skip the canary, the coverage assertion and the re-auth
    # reminder. The settings become the backup's finding instead, which withholds the
    # ping, and the real client builder is never reached.
    lake, _ = _uploaded(tmp_path / "lake")
    monkeypatch.setattr(bucket, "_build_client", lambda cfg: pytest.fail("no client is built"))

    code, pinger, canaries = _sunday_cli(tmp_path, monkeypatch, lake, target=target, keys=keys)

    assert code == 1
    assert pinger.urls == []
    assert canaries
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    finding = [line for line in captured.out.splitlines() if "cannot be used" in line]
    assert finding and all(named in line for line in finding)
    assert "secret-bucket-key" not in captured.out


def test_a_client_that_cannot_be_built_is_a_sunday_finding(tmp_path, capsys, monkeypatch):
    # The region check runs first, and anything botocore still refuses at build time
    # lands the same way rather than as a traceback.
    from botocore.exceptions import InvalidRegionError

    lake, _ = _uploaded(tmp_path / "lake")

    def refuse(cfg):
        raise InvalidRegionError(region_name="us-east-2")

    monkeypatch.setattr(bucket, "_build_client", refuse)

    code, pinger, canaries = _sunday_cli(tmp_path, monkeypatch, lake)

    assert code == 1
    assert pinger.urls == [] and canaries
    printed = capsys.readouterr().out
    assert "cannot be used" in printed and "InvalidRegionError" in printed


def test_a_whole_object_digest_with_no_checksum_type_still_matches(tmp_path):
    # S3 has not always returned ChecksumType. A value that decodes to the manifest's 32
    # digest bytes cannot be a composite, whose "-N" suffix fails the decode, so it
    # counts. The live check prints the type S3 returns today.
    lake, client = _uploaded(tmp_path / "lake")
    client.store(_key(PARTITION), (lake / PARTITION).read_bytes(), checksum_type=None)
    assert bucket_scrub(lake, TARGET, client).ok
