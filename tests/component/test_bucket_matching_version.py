"""Reading the version a bucket manifest names after an upload stopped part-way.

Marketlake #838. A nightly upload PUTs each pending file and then ``manifest.jsonl`` last. One
that stops in between leaves a rewritten ledger's current version newer than the entry the
bucket's manifest still holds, while the version that entry names is still in the versioned
bucket. Every reader that checks a bucket object against the bucket manifest's entry asks
``matching_version`` for that version once the current one has failed its check. The cases
follow the issue's Verification list.

1. A nightly upload stopped after two rewritten ledgers' PUTs, then each of ``restore``,
   ``restore-for-reading``, the windowed rebuild and ``resync --apply``. Each takes the version
   the entry names and finishes.
2. The same with no matching version left fails each, with the line for that cause.
3. The same on a role without the version grants fails each as before #838, with the same exit
   code and one more line naming the grants.
7. The resolver's own rules: paging, the exact key, delete markers, a version with no stored
   checksum, a HEAD answering absent, the current version, and the first match ending it.
8. A restore with nothing mismatched sends no version listing and names no version.
9. An unmanifested file's mismatch never reaches the resolver.

5. The next nightly upload on whichever host uploads next puts the named bytes back, and the
   bucket scrub then comes back clean, after a whole restore, a windowed rebuild, a resync, a
   takeover by a host already level, a restore resumed after a failed run, and a resync run
   again after a crash before its manifest rewrite. Only a run that downloaded an older version
   names it.

Item 4, the resync's own cases, sits in ``test_bucket_resync.py``. Item 6, the heal's own
rules, sits in ``test_bucket_upload.py``, and item 10, the live check, in
``test_bucket_live_check.py``. Expected bytes
are read from the lake or the fake bucket's store, never through the code under test.
"""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import json
import shutil
from datetime import date, datetime
from pathlib import Path

import pytest

from lake import bucket
from lake.bucket import (
    BucketReadError,
    UploadDeadline,
    bucket_scrub,
    first_upload,
    matching_version,
    nightly_upload,
    restore_lake,
    resync,
)
from lake.calendar import MARKET_TZ
from lake.config import BucketTarget
from lake.lock import lake_lock
from lake.manifest import append_manifest, sha256_file
from lake.trimmed import append_trimmed, trim_line
from tests.support.bucket import FakeS3, client_error, unreachable
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.lake import FixtureLake

TARGET = BucketTarget(bucket="lake-backup", prefix="lake")
D1 = date(2026, 8, 24)
D2 = date(2026, 8, 25)
D3 = date(2026, 8, 26)
SPY_1 = "chains/ticker=SPY/date=2026-08-24.parquet"
SPY_2 = "chains/ticker=SPY/date=2026-08-25.parquet"
SPY_3 = "chains/ticker=SPY/date=2026-08-26.parquet"
QUARANTINE = "quarantine.jsonl"
TRIMMED = "trimmed.jsonl"
REPORT = "reports/date=2026-08-24.md"
WORK = ".marketlake-restoring"
CALENDAR = weekday_sessions(date(2026, 8, 24), date(2026, 8, 31))
MONDAY_19 = datetime(2026, 8, 24, 19, 0, tzinfo=MARKET_TZ)
TUESDAY_19 = datetime(2026, 8, 25, 19, 0, tzinfo=MARKET_TZ)
WEDNESDAY_19 = datetime(2026, 8, 26, 19, 0, tzinfo=MARKET_TZ)
WEDNESDAY_20 = datetime(2026, 8, 26, 20, 0, tzinfo=MARKET_TZ)
THURSDAY_19 = datetime(2026, 8, 27, 19, 0, tzinfo=MARKET_TZ)
STAMP = "2026-08-25T19:00:00-04:00"
PLENTY = 10**12
GRANTS = "s3:ListBucketVersions and s3:GetObjectVersion"


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _b64(data: bytes) -> str:
    return base64.b64encode(hashlib.sha256(data).digest()).decode()


# -- the stopped upload ------------------------------------------------------------


@dataclasses.dataclass
class Stopped:
    """A primary lake, a shadow restored from its first upload, and the bucket between them.

    ``named`` holds the bytes the bucket's manifest names for each rewritten ledger, which the
    primary's first night uploaded whole. ``newer`` holds what the second night PUT before it
    stopped. ``level`` is a copy of the primary taken after its first night, a host already
    level with the bucket that can take over with no restore.
    """

    primary: Path
    shadow: Path
    client: FakeS3
    named: dict[str, bytes]
    newer: dict[str, bytes]
    level: Path


def _verdict(root: Path, partition: str) -> None:
    """Append a battery verdict to the quarantine ledger and record it, as the battery does."""
    path = root / QUARANTINE
    with path.open("a") as handle:
        handle.write(json.dumps({"partition": partition, "verdict": "clean"}) + "\n")
    append_manifest(
        root,
        partition=QUARANTINE,
        source="battery",
        sha256=sha256_file(path),
        rows=1,
        fetched_at=None,
    )


def _trim(root: Path, client: FakeS3, rel: str) -> None:
    """Trim a partition the way ``lake.trim`` does: the trim line, then the unlink."""
    line = trim_line(
        rel,
        sha256=sha256_file(root / rel),
        version_id=client.versions(TARGET.key(rel))[-1].version_id,
        verified_at=STAMP,
        trimmed_at=STAMP,
    )
    with lake_lock(root):
        append_trimmed(root, line, source="test-trim", fetched_at=STAMP)
    (root / rel).unlink()


def _stopped(tmp_path: Path, *, stop: bool = True) -> Stopped:
    """Two nights on the primary, the second stopped after its ledgers' PUTs.

    1. Monday, the first upload seeds the bucket, and the shadow is restored from it.
    2. Tuesday, a verdict and a trim rewrite ``quarantine.jsonl`` and ``trimmed.jsonl``, and the
       nightly upload finishes, so the bucket's manifest names those bytes.
    3. Wednesday, another verdict and another trim rewrite both again. The nightly upload PUTs
       both, and its deadline passes on the second PUT, so it stops before manifest.jsonl. With
       ``stop`` false it finishes instead.
    """
    primary = (
        FixtureLake(tmp_path / "primary")
        .with_chains("SPY", D1)
        .with_chains("SPY", D2)
        .with_chains("SPY", D3)
        .with_quarantine({"partition": SPY_1, "verdict": "clean"})
        .build()
    )
    client = FakeS3()
    first_upload(primary, TARGET, client=client, clock=ManualClock(MONDAY_19), calendar=CALENDAR)
    shadow = tmp_path / "shadow"
    assert restore_lake(shadow, TARGET, client=client, free_space=lambda _: PLENTY).restored

    _verdict(primary, SPY_2)
    _trim(primary, client, SPY_1)
    nightly_upload(primary, TARGET, client=client, clock=ManualClock(TUESDAY_19), calendar=CALENDAR)
    named = {rel: (primary / rel).read_bytes() for rel in (QUARANTINE, TRIMMED)}
    level = tmp_path / "level"
    shutil.copytree(primary, level)

    _verdict(primary, SPY_3)
    _trim(primary, client, SPY_2)
    newer = {rel: (primary / rel).read_bytes() for rel in (QUARANTINE, TRIMMED)}
    clock = ManualClock(WEDNESDAY_19)
    if stop:

        def stall(kwargs: dict, data: bytes) -> None:
            if kwargs["Key"] == TARGET.key(TRIMMED):
                clock.advance(bucket.NIGHTLY_UPLOAD_BUDGET.total_seconds())

        client.on_put = stall
        with pytest.raises(UploadDeadline):
            nightly_upload(primary, TARGET, client=client, clock=clock, calendar=CALENDAR)
        client.on_put = None
        assert client.put_keys()[-2:] == [TARGET.key(QUARANTINE), TARGET.key(TRIMMED)]
        for rel in (QUARANTINE, TRIMMED):
            assert client.body(TARGET.key(rel)) == newer[rel]
    else:
        nightly_upload(primary, TARGET, client=client, clock=clock, calendar=CALENDAR)
    client.calls.clear()
    return Stopped(primary, shadow, client, named, newer, level)


def _expire(client: FakeS3, *rels: str) -> None:
    """Drop every noncurrent version of each path, as the bucket's 30-day expiry would."""
    for rel in rels:
        key = TARGET.key(rel)
        client.objects[key] = client.objects[key][-1:]


def _deny(client: FakeS3) -> None:
    """The role as it was before marketlake #838's apply: neither version grant."""
    client.deny_list_versions = True
    client.deny_versioned_get = True


def _version(client: FakeS3, rel: str, data: bytes) -> str:
    """The id of the version of ``rel`` holding ``data``."""
    (found,) = [v.version_id for v in client.versions(TARGET.key(rel)) if v.body == data]
    return found


def _main(tmp_path: Path, lake_root: Path, client: FakeS3, monkeypatch, argv: list[str]) -> int:
    """``main`` with the client patched in, on a config naming ``lake_root``.

    A ``--window`` argument is taken out and written as ``lake_window_sessions``.
    """
    config = write_config(
        tmp_path,
        lake_root,
        bucket_access_key_id="AKIDCONFIG",
        bucket_secret_access_key="secret-bucket-key",
        bucket_region="us-east-2",
    )
    if "--window" in argv:
        at = argv.index("--window")
        config.write_text(config.read_text() + f"lake_window_sessions: {argv[at + 1]}\n")
        argv = argv[:at] + argv[at + 2 :]
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    argv = [*argv, "--config", str(config), "--target", "s3://lake-backup/lake"]
    return bucket.main(argv, clock=ManualClock(WEDNESDAY_20), calendar=CALENDAR)


def _whole(tmp_path: Path, lake: Stopped, monkeypatch) -> int:
    return _main(tmp_path, lake.primary, lake.client, monkeypatch, ["restore", str(tmp_path / "d")])


def _windowed(tmp_path: Path, lake: Stopped, monkeypatch) -> int:
    argv = ["restore", str(tmp_path / "d"), "--window", "22"]
    return _main(tmp_path, lake.primary, lake.client, monkeypatch, argv)


def _reading(tmp_path: Path, lake: Stopped, monkeypatch) -> int:
    argv = ["restore-for-reading", str(tmp_path / "d"), "--surface", "chains", "--ticker", "SPY"]
    argv += ["--from", D3.isoformat(), "--to", D3.isoformat()]
    return _main(tmp_path, lake.primary, lake.client, monkeypatch, argv)


def _resync(lake: Stopped, *, apply: bool = True) -> bucket.ResyncSummary:
    return resync(
        lake.shadow,
        TARGET,
        client=lake.client,
        clock=ManualClock(WEDNESDAY_20),
        calendar=CALENDAR,
        apply=apply,
        job_probe=lambda label: False,
        geteuid=lambda: 501,
        free_space=lambda _: PLENTY,
    )


def _took(label: str, rel: str, version: str) -> str:
    return f"{label}: took {rel} from bucket version {version}, the one its manifest.jsonl entry "


# -- 1. each reader takes the version the entry names --------------------------------


@pytest.mark.parametrize(
    ("run", "label", "rels"),
    [
        pytest.param(_whole, "restore", (QUARANTINE, TRIMMED), id="restore"),
        pytest.param(_windowed, "restore", (QUARANTINE, TRIMMED), id="windowed-rebuild"),
        pytest.param(_reading, "restore-for-reading", (QUARANTINE,), id="restore-for-reading"),
    ],
)
def test_a_restore_after_a_stopped_upload_takes_the_versions_the_manifest_names(
    tmp_path, monkeypatch, capsys, run, label, rels
):
    lake = _stopped(tmp_path)

    code = run(tmp_path, lake, monkeypatch)

    assert code == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    out = captured.out.splitlines()
    for line, rel in zip(out, rels, strict=False):
        assert line.startswith(_took(label, rel, _version(lake.client, rel, lake.named[rel])))
    assert len(out) == len(rels) + 1
    for rel in rels:
        assert (tmp_path / "d" / rel).read_bytes() == lake.named[rel]


def test_the_windowed_rebuild_leaves_out_what_the_named_ledger_trimmed(tmp_path, monkeypatch):
    # The named ledger trims SPY_1 and not SPY_2, which only the newer ledger trims.
    lake = _stopped(tmp_path)

    assert _windowed(tmp_path, lake, monkeypatch) == 0

    assert not (tmp_path / "d" / SPY_1).exists()
    assert (tmp_path / "d" / SPY_2).is_file()


def test_a_resync_after_a_stopped_upload_takes_the_versions_the_manifest_names(tmp_path):
    lake = _stopped(tmp_path)

    summary = _resync(lake)

    assert summary.applied
    for rel in (QUARANTINE, TRIMMED):
        assert (lake.shadow / rel).read_bytes() == lake.named[rel]
        version = _version(lake.client, rel, lake.named[rel])
        assert any(
            line.startswith(f"took {rel} from bucket version {version}, ")
            for line in (summary.warnings)
        )


# -- 2. no version matches -------------------------------------------------------------


@pytest.mark.parametrize(
    ("run", "label", "failed"),
    [
        pytest.param(_whole, "restore", (QUARANTINE, TRIMMED), id="restore"),
        pytest.param(_reading, "restore-for-reading", (QUARANTINE,), id="restore-for-reading"),
    ],
)
def test_a_restore_with_no_matching_version_fails_each_file(
    tmp_path, monkeypatch, capsys, run, label, failed
):
    lake = _stopped(tmp_path)
    _expire(lake.client, QUARANTINE, TRIMMED)

    code = run(tmp_path, lake, monkeypatch)

    assert code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    err = captured.err.splitlines()
    for line, rel in zip(err, failed, strict=False):
        assert line.startswith(f"{label}: does not match its SHA-256: {rel}")
    assert GRANTS not in captured.err
    if label == "restore-for-reading":
        assert err[len(failed)].startswith(f"{label}: no version in the bucket matches ")
        assert str(tmp_path / "d" / WORK) in err[len(failed)]
    assert len(err) == len(failed) + (2 if label == "restore-for-reading" else 1)


def test_a_windowed_rebuild_with_no_matching_ledger_refuses_with_the_hand_copy(
    tmp_path, monkeypatch, capsys
):
    lake = _stopped(tmp_path)
    _expire(lake.client, TRIMMED)

    with pytest.raises(SystemExit) as exc:
        _windowed(tmp_path, lake, monkeypatch)

    assert exc.value.code == 2
    (line,) = capsys.readouterr().err.splitlines()
    assert "no older version matches it" in line
    assert "from outside the bucket" in line
    assert _sha(lake.named[TRIMMED]) in line
    assert str(tmp_path / "d" / WORK / TRIMMED) in line
    assert GRANTS not in line


def test_a_resync_with_no_matching_ledger_refuses_naming_the_cause(tmp_path):
    lake = _stopped(tmp_path)
    _expire(lake.client, TRIMMED)

    with pytest.raises(bucket.ResyncRefused) as refused:
        _resync(lake)

    message = str(refused.value)
    assert "no older version matches it" in message
    assert GRANTS not in message


# -- 3. the version calls refused --------------------------------------------------------


@pytest.mark.parametrize(
    ("run", "label", "failed"),
    [
        pytest.param(_whole, "restore", (QUARANTINE, TRIMMED), id="restore"),
        pytest.param(_reading, "restore-for-reading", (QUARANTINE,), id="restore-for-reading"),
    ],
)
def test_a_restore_whose_lookup_is_refused_keeps_its_failures_and_exit_1(
    tmp_path, monkeypatch, capsys, run, label, failed
):
    lake = _stopped(tmp_path)
    _deny(lake.client)

    code = run(tmp_path, lake, monkeypatch)

    assert code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    err = captured.err.splitlines()
    for line, rel in zip(err, failed, strict=False):
        assert line.startswith(f"{label}: does not match its SHA-256: {rel}")
    grants = err[len(failed)]
    assert grants.startswith(f"{label}: the bucket refused the lookup of an older version ")
    assert "(AccessDenied)" in grants and GRANTS in grants
    assert "no version in the bucket matches" not in captured.err
    assert len(err) == len(failed) + 2
    # The working directory keeps every file that verified, for the next run.
    assert (tmp_path / "d" / WORK / SPY_3).is_file()


def test_a_windowed_rebuild_whose_lookup_is_refused_refuses_with_the_grants(
    tmp_path, monkeypatch, capsys
):
    lake = _stopped(tmp_path)
    _deny(lake.client)

    with pytest.raises(SystemExit) as exc:
        _windowed(tmp_path, lake, monkeypatch)

    assert exc.value.code == 2
    (line,) = capsys.readouterr().err.splitlines()
    assert "refused the lookup of an older version" in line and GRANTS in line
    assert "from the console" in line
    assert _sha(lake.named[TRIMMED]) in line
    # The working directory the console repair needs is left, holding the bucket's manifest.
    assert (tmp_path / "d" / WORK / "manifest.jsonl").is_file()


def test_a_resync_whose_lookup_is_refused_refuses_with_the_grants(tmp_path):
    lake = _stopped(tmp_path)
    _deny(lake.client)
    before = sorted(p.name for p in lake.shadow.rglob("*"))

    with pytest.raises(bucket.ResyncRefused) as refused:
        _resync(lake)

    message = str(refused.value)
    assert "refused the lookup of an older version" in message and GRANTS in message
    assert sorted(p.name for p in lake.shadow.rglob("*")) == before


# -- 7. the resolver's rules -------------------------------------------------------------


KEY = TARGET.key(TRIMMED)
ENTRY = b"the bytes the entry names\n"


def _head_ids(client: FakeS3) -> list[str]:
    return [kwargs["VersionId"] for name, kwargs in client.calls if name == "head_object"]


def _listings(client: FakeS3) -> list[dict]:
    return [kwargs for name, kwargs in client.calls if name == "list_object_versions"]


def _ids(client: FakeS3, key: str = KEY) -> list[str]:
    return [version.version_id for version in client.versions(key)]


def test_a_match_past_the_first_page_is_found(tmp_path):
    client = FakeS3(page_size=2)
    client.store(KEY, ENTRY)
    for n in range(3):
        client.store(KEY, f"later {n}\n".encode())
    v1, _v2, v3, _v4 = _ids(client)

    found = matching_version(client, TARGET, TRIMMED, _sha(ENTRY))

    assert found == bucket.MatchingVersion(version_id=v1, size=len(ENTRY))
    second = _listings(client)[1]
    assert (second["KeyMarker"], second["VersionIdMarker"]) == (KEY, v3)
    assert len(_listings(client)) == 2


def test_a_sibling_key_is_never_a_candidate(tmp_path):
    # The sibling holds a noncurrent version with the entry's own checksum, and the exact key
    # holds no match, so only the exact-key filter keeps the answer None.
    client = FakeS3()
    client.store(KEY, b"no match\n")
    client.store(KEY, b"current\n")
    client.store(KEY + ".x", ENTRY)
    client.store(KEY + ".x", b"sibling current\n")
    sibling_match = _ids(client, KEY + ".x")[0]

    assert matching_version(client, TARGET, TRIMMED, _sha(ENTRY)) is None

    assert sibling_match not in _head_ids(client)
    assert _head_ids(client) == [_ids(client)[0]]


def test_the_lookup_stops_once_the_listing_moves_past_the_key(tmp_path):
    client = FakeS3(page_size=1)
    client.store(KEY, b"no match\n")
    client.store(KEY, b"current\n")
    for n in range(3):
        client.store(KEY + ".x", f"sibling {n}\n".encode())

    assert matching_version(client, TARGET, TRIMMED, _sha(ENTRY)) is None

    # The key's two versions, then the page that reaches the sibling and ends it.
    assert len(_listings(client)) == 3


def test_a_delete_marker_is_never_read(tmp_path):
    client = FakeS3()
    client.store(KEY, ENTRY)
    marker = client.delete_marker(KEY)
    client.store(KEY, b"current\n")

    found = matching_version(client, TARGET, TRIMMED, _sha(ENTRY))

    assert found is not None and found.version_id == _ids(client)[0]
    assert marker not in _head_ids(client)


def test_a_version_stored_with_no_checksum_never_matches(tmp_path):
    client = FakeS3()
    client.store(KEY, ENTRY)
    client.store(KEY, ENTRY, checksum=None)
    client.store(KEY, b"current\n")
    v1, v2, _v3 = _ids(client)

    found = matching_version(client, TARGET, TRIMMED, _sha(ENTRY))

    assert found is not None and found.version_id == v1
    assert _head_ids(client) == [v2, v1]


def test_a_version_with_only_a_composite_checksum_never_matches(tmp_path):
    client = FakeS3()
    client.store(KEY, ENTRY, checksum=_b64(ENTRY), checksum_type="COMPOSITE")
    client.store(KEY, b"current\n")

    assert matching_version(client, TARGET, TRIMMED, _sha(ENTRY)) is None


def test_a_head_answering_absent_skips_that_candidate(tmp_path):
    client = FakeS3()
    client.store(KEY, ENTRY)
    client.store(KEY, ENTRY)
    client.store(KEY, b"current\n")
    v1, v2, _v3 = _ids(client)
    real_head = client.head_object

    def head_object(**kwargs):
        if kwargs.get("VersionId") == v2:
            # Expired between the listing and the HEAD.
            client.calls.append(("head_object", dict(kwargs)))
            raise client_error("404", "HeadObject", 404)
        return real_head(**kwargs)

    client.head_object = head_object

    found = matching_version(client, TARGET, TRIMMED, _sha(ENTRY))

    assert found is not None and found.version_id == v1


def test_the_current_version_is_never_a_candidate(tmp_path):
    # Rot at rest: the current version's stored checksum still names the entry's bytes.
    client = FakeS3()
    client.store(KEY, b"no match\n")
    client.store(KEY, b"rotted\n", checksum=_b64(ENTRY))
    v1, v2 = _ids(client)

    assert matching_version(client, TARGET, TRIMMED, _sha(ENTRY)) is None

    assert _head_ids(client) == [v1]


def test_an_id_that_names_no_version_is_never_asked_for(tmp_path):
    client = FakeS3()
    client.store(KEY, b"current\n")
    real_list = client.list_object_versions

    def list_object_versions(**kwargs):
        page = real_list(**kwargs)
        extra = {"Key": KEY, "IsLatest": False, "Size": 1}
        page["Versions"] = (
            page["Versions"]
            + [{**extra, "VersionId": vid} for vid in ("null", "NULL", " ", "")]
            + [extra]
        )
        return page

    client.list_object_versions = list_object_versions

    assert matching_version(client, TARGET, TRIMMED, _sha(ENTRY)) is None

    assert _head_ids(client) == []


def test_the_first_match_ends_the_lookup(tmp_path):
    client = FakeS3(page_size=2)
    client.store(KEY, ENTRY)
    client.store(KEY, ENTRY)
    client.store(KEY, b"current\n")
    _v1, v2, _v3 = _ids(client)

    found = matching_version(client, TARGET, TRIMMED, _sha(ENTRY))

    assert found is not None and found.version_id == v2
    assert _head_ids(client) == [v2]
    assert len(_listings(client)) == 1


@pytest.mark.parametrize(
    ("deny", "kind", "code"),
    [
        pytest.param({"deny_list_versions": True}, "refused", "AccessDenied", id="list"),
        pytest.param({"deny_versioned_get": True}, "refused", "403", id="head"),
    ],
)
def test_a_refused_lookup_raises_a_refused_read_error(tmp_path, deny, kind, code):
    client = FakeS3(**deny)
    client.store(KEY, ENTRY)
    client.store(KEY, b"current\n")

    with pytest.raises(BucketReadError) as raised:
        matching_version(client, TARGET, TRIMMED, _sha(ENTRY))

    assert (raised.value.kind, raised.value.code, raised.value.rel) == (kind, code, TRIMMED)


def test_an_unreachable_bucket_raises_an_unreachable_read_error(tmp_path):
    client = FakeS3()
    client.fail_with = unreachable()

    with pytest.raises(BucketReadError) as raised:
        matching_version(client, TARGET, TRIMMED, _sha(ENTRY))

    assert raised.value.kind == "unreachable"


def test_a_bug_in_the_listing_raises_as_itself(tmp_path):
    client = FakeS3()
    client.fail_with = KeyError("not a bucket failure")

    with pytest.raises(KeyError):
        matching_version(client, TARGET, TRIMMED, _sha(ENTRY))


# -- 8. nothing mismatched sends no version request -------------------------------------


@pytest.mark.parametrize("run", [_whole, _windowed, _reading], ids=["restore", "windowed", "read"])
def test_a_restore_with_nothing_mismatched_sends_no_version_request(
    tmp_path, monkeypatch, capsys, run
):
    lake = _stopped(tmp_path, stop=False)
    _deny(lake.client)

    assert run(tmp_path, lake, monkeypatch) == 0

    assert lake.client.versioned_calls() == []
    assert "took " not in capsys.readouterr().out


def test_a_resync_with_nothing_mismatched_sends_no_version_request(tmp_path):
    lake = _stopped(tmp_path, stop=False)
    _deny(lake.client)

    summary = _resync(lake)

    assert summary.applied
    assert summary.warnings == []
    assert lake.client.versioned_calls() == []


# -- 9. an unmanifested file -------------------------------------------------------------


def test_an_unmanifested_file_s_mismatch_never_reaches_the_resolver(tmp_path):
    # The report's older version holds the bytes its current version's stored checksum names,
    # so a resolver asked about it would find a match. An unmanifested file is checked against
    # its current version alone.
    lake = _stopped(tmp_path, stop=False)
    key = TARGET.key(REPORT)
    lake.client.store(key, b"the report\n")
    lake.client.store(key, b"rotted report\n", checksum=_b64(b"the report\n"))

    summary = restore_lake(tmp_path / "d", TARGET, client=lake.client, free_space=lambda _: PLENTY)

    assert summary.failures == [(REPORT, "does not match its SHA-256")]
    assert summary.recovered == []
    assert lake.client.versioned_calls() == []


# -- 5. the next nightly upload heals the bucket -----------------------------------------


class Crash(Exception):
    """A process death injected before the resync's manifest rewrite. Nothing catches it."""


def _heals(lake: Stopped, host: Path) -> None:
    """The bucket scrub pages on the two ledgers, the host's next upload heals, and it is clean."""
    before = bucket_scrub(host, TARGET, lake.client)
    assert sorted(before.sha_mismatches) == [QUARANTINE, TRIMMED]
    lake.client.calls.clear()

    summary = nightly_upload(
        host, TARGET, client=lake.client, clock=ManualClock(THURSDAY_19), calendar=CALENDAR
    )

    assert summary.healed == [QUARANTINE, TRIMMED]
    assert summary.unhealed == []
    assert lake.client.put_keys() == [TARGET.key(QUARANTINE), TARGET.key(TRIMMED)]
    for rel in (QUARANTINE, TRIMMED):
        assert lake.client.body(TARGET.key(rel)) == lake.named[rel]
    after = bucket_scrub(host, TARGET, lake.client)
    assert after.sha_mismatches == () and after.ok


def _recovered(lake: Stopped) -> list[tuple[str, str]]:
    return [(rel, _version(lake.client, rel, lake.named[rel])) for rel in (QUARANTINE, TRIMMED)]


@pytest.mark.parametrize("windowed", [False, True], ids=["whole", "windowed"])
def test_the_next_upload_after_a_restore_heals_the_bucket(tmp_path, windowed):
    lake = _stopped(tmp_path)
    host = tmp_path / "host"

    summary = restore_lake(
        host,
        TARGET,
        client=lake.client,
        free_space=lambda _: PLENTY,
        skip_designed_absences=windowed,
    )

    assert summary.restored
    assert summary.recovered == _recovered(lake)
    _heals(lake, host)


def test_the_next_upload_after_a_resync_heals_the_bucket(tmp_path):
    lake = _stopped(tmp_path)

    assert _resync(lake).applied

    _heals(lake, lake.shadow)


def test_a_level_host_that_takes_over_heals_the_bucket_with_no_restore(tmp_path):
    lake = _stopped(tmp_path)

    _heals(lake, lake.level)


def test_a_resumed_restore_names_the_older_versions_only_on_the_run_that_took_them(
    tmp_path, monkeypatch, capsys
):
    lake = _stopped(tmp_path)
    versions = lake.client.versions(TARGET.key(SPY_3))
    good = versions[-1]
    versions[-1] = dataclasses.replace(good, body=b"rotted at rest")
    argv = ["restore", str(tmp_path / "d")]

    assert _main(tmp_path, lake.primary, lake.client, monkeypatch, argv) == 1
    first = capsys.readouterr()
    for rel, version in _recovered(lake):
        assert _took("restore", rel, version) in first.out
    assert f"restore: does not match its SHA-256: {SPY_3}" in first.err

    versions[-1] = good
    assert _main(tmp_path, lake.primary, lake.client, monkeypatch, argv) == 0
    second = capsys.readouterr()
    assert "took " not in second.out
    assert second.err == ""

    _heals(lake, tmp_path / "d")


def test_a_resync_run_again_after_a_crash_names_nothing_and_the_upload_heals(tmp_path, monkeypatch):
    lake = _stopped(tmp_path)

    def crash(*args, **kwargs):
        raise Crash

    monkeypatch.setattr(bucket, "_rewrite_manifest", crash)
    with pytest.raises(Crash):
        _resync(lake)
    monkeypatch.undo()
    for rel in (QUARANTINE, TRIMMED):
        assert (lake.shadow / rel).read_bytes() == lake.named[rel]

    again = _resync(lake)

    assert again.applied
    assert again.warnings == []
    _heals(lake, lake.shadow)


def test_a_delete_marker_is_never_read_when_no_version_matches(tmp_path):
    # With no version to match, a lookup that read delete markers too would HEAD the marker,
    # which S3 answers 405.
    client = FakeS3()
    client.store(KEY, b"no match\n")
    marker = client.delete_marker(KEY)
    client.store(KEY, b"current\n")

    assert matching_version(client, TARGET, TRIMMED, _sha(ENTRY)) is None

    assert marker not in _head_ids(client)


def test_an_older_version_whose_bytes_rotted_is_not_taken(tmp_path):
    # Its stored checksum names the entry, so the lookup picks it, and only its bytes can
    # refuse it.
    lake = _stopped(tmp_path)
    key = TARGET.key(QUARANTINE)
    versions = lake.client.versions(key)
    named = _version(lake.client, QUARANTINE, lake.named[QUARANTINE])
    index = [v.version_id for v in versions].index(named)
    versions[index] = dataclasses.replace(versions[index], body=b"rotted at rest\n")

    summary = restore_lake(tmp_path / "d", TARGET, client=lake.client, free_space=lambda _: PLENTY)

    assert (QUARANTINE, "does not match its SHA-256") in summary.failures
    assert QUARANTINE not in dict(summary.recovered)
    assert not (tmp_path / "d" / WORK / (QUARANTINE + ".part")).exists()
