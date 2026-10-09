"""The resync, ``python -m lake.bucket resync``, against the fake S3 client.

Marketlake #832. A host that is about to become primary again finds the bucket's
``manifest.jsonl`` holding the other host's sessions past the entries both share, and its own
lake holding a tail the shadow's sweep wrote. The resync drops that tail, downloads what the
bucket's tail names, and leaves the lake's manifest equal to the bucket's bytes. Each test
builds the two lakes the way the hosts would: the laptop uploads, the VM is restored from the
bucket and uploads a session of its own, and the laptop's sweep grows a tail.

The cases follow item 4 of the issue.

1. The classification: level, append-only, a rewind, a hand repair, and a copy that is absent,
   carries no whole entry, or changed between its HEAD and its GET.
2. The plan: what is downloaded, what is skipped, and what is deleted.
3. The refusals on this lake's own tail, each cleared by moving the file out.
4. The checks before any download: the listing, the stored SHA-256 and the journal reserve.
5. The guards, and the command's lines.
6. ``--apply``: the commit keeps the manifest's inode, a change during the downloads refuses
   at the commit, every stop leaves no temp file, and a re-run finishes after a crash.
7. The job probe a systemd host asks, which reads ``ActiveState``.
8. Both directions end to end against one fake bucket, the first test that uploads from a
   restored lake.

Every refusal is one line, so each test that meets one asserts ``"\\n" not in message``.
Expected digests are read from the files the test wrote, never through the code under test.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from pathlib import Path

import pytest

from lake import bucket
from lake.bucket import (
    FIRST_UPLOAD_COMMAND,
    ResyncRefused,
    first_upload,
    nightly_upload,
    restore_lake,
    resync,
)
from lake.calendar import MARKET_TZ
from lake.config import BucketTarget
from lake.manifest import append_manifest, manifest_path, sha256_file
from tests.support.bucket import FakeS3, client_error
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.lake import FixtureLake

TARGET = BucketTarget(bucket="lake-backup", prefix="lake")
MANIFEST_KEY = TARGET.key("manifest.jsonl")
D1 = date(2026, 8, 24)
D2 = date(2026, 8, 25)
D3 = date(2026, 8, 26)
CALENDAR = weekday_sessions(date(2026, 8, 24), date(2026, 8, 31))
# Evenings after the 18:30 sweep: Monday, when the laptop uploads, Tuesday, when the VM
# uploads its session and the laptop resyncs, and Wednesday, for the way back.
MONDAY_19 = datetime(2026, 8, 24, 19, 0, tzinfo=MARKET_TZ)
TUESDAY_19 = datetime(2026, 8, 25, 19, 0, tzinfo=MARKET_TZ)
TUESDAY_20 = datetime(2026, 8, 25, 20, 0, tzinfo=MARKET_TZ)
# A Sunday inside the scrub window, and a Tuesday inside the session.
SUNDAY_20 = datetime(2026, 8, 30, 20, 0, tzinfo=MARKET_TZ)
TUESDAY_10 = datetime(2026, 8, 25, 10, 0, tzinfo=MARKET_TZ)
# Wednesday's open at 09:30 less the 15-minute in-flight allowance and the 15-minute margin,
# written out rather than computed from the constants under test.
WEDNESDAY_BOUND = "2026-08-26T09:00:00-04:00"
PLENTY = 10**12
OWNER = 501
SPY_1 = "chains/ticker=SPY/date=2026-08-24.parquet"
SPY_2 = "chains/ticker=SPY/date=2026-08-25.parquet"
QUOTES_2 = "quotes/ticker=SPY/date=2026-08-25.parquet"
BARS_2 = "bars/ticker=SPY/freq=1d/date=2026-08-25.parquet"
QUARANTINE = "quarantine.jsonl"
SPANS = "reference/capture_spans.parquet"
SCHEMA_LEDGER = "reference/schema_versions.parquet"
SEGMENT_2 = "journal/date=2026-08-25/surface=chains/ticker=SPY/seg-20260825T1330-41.arrows"
KEYS = (
    "bucket_access_key_id: AKIDCONFIG\n"
    "bucket_secret_access_key: secret-bucket-key\n"
    "bucket_region: us-east-2\n"
)


# -- building the two hosts -------------------------------------------------------


def _record(root: Path, rel: str, data: bytes, *, source: str = "compaction") -> str:
    """Write ``data`` at ``rel`` and append its manifest entry, the way a lake writer would.

    It returns the file's SHA-256, read from the bytes written.
    """
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    sha = hashlib.sha256(data).hexdigest()
    append_manifest(root, partition=rel, source=source, sha256=sha, rows=100, fetched_at=None)
    return sha


def _laptop(tmp_path: Path) -> tuple[Path, FakeS3]:
    """The laptop's lake, uploaded whole to a fresh bucket on Monday evening."""
    root = (
        FixtureLake(tmp_path / "laptop")
        .with_chains("SPY", D1)
        .with_quotes("SPY", D1)
        .with_quarantine({"partition": SPY_1, "check": "row_count", "verdict": "clean"})
        .build()
    )
    client = FakeS3()
    first_upload(root, TARGET, client=client, clock=ManualClock(MONDAY_19), calendar=CALENDAR)
    return root, client


def _vm(tmp_path: Path, client: FakeS3) -> Path:
    """The VM's lake, restored from the bucket the laptop uploaded."""
    summary = restore_lake(tmp_path / "vm", TARGET, client=client, free_space=lambda _: PLENTY)
    assert summary.restored
    return tmp_path / "vm"


def _vm_session(vm: Path, client: FakeS3) -> dict[str, bytes]:
    """The VM's Tuesday session, sealed and uploaded on Tuesday evening."""
    files = {SPY_2: b"vm chains 2026-08-25", QUOTES_2: b"vm quotes 2026-08-25"}
    for rel, data in files.items():
        _record(vm, rel, data)
    nightly_upload(vm, TARGET, client=client, clock=ManualClock(TUESDAY_19), calendar=CALENDAR)
    return files


def _laptop_sweep(laptop: Path) -> None:
    """The shadow laptop's Tuesday sweep: a bars partition and a battery verdict."""
    _record(laptop, BARS_2, b"laptop bars 2026-08-25", source="sweep")
    quarantine = laptop / QUARANTINE
    line = {"partition": SPY_2, "check": "row_count", "verdict": "clean"}
    with quarantine.open("a") as handle:
        handle.write(json.dumps(line) + "\n")
    append_manifest(
        laptop,
        partition=QUARANTINE,
        source="battery",
        sha256=sha256_file(quarantine),
        rows=100,
        fetched_at=None,
    )


def _switched(tmp_path: Path) -> tuple[Path, Path, FakeS3, dict[str, bytes]]:
    """Both hosts after one VM session, with the laptop's sweep tail in place."""
    laptop, client = _laptop(tmp_path)
    vm = _vm(tmp_path, client)
    files = _vm_session(vm, client)
    _laptop_sweep(laptop)
    client.calls.clear()
    return laptop, vm, client, files


def _idle(label: str) -> bool:
    """A job probe that finds no job executing."""
    return False


def _resync(
    root: Path,
    client: FakeS3,
    *,
    now: datetime | ManualClock = TUESDAY_20,
    free: int = PLENTY,
    euid: int = OWNER,
    apply: bool = False,
    probe=_idle,
) -> bucket.ResyncSummary:
    return resync(
        root,
        TARGET,
        client=client,
        clock=now if isinstance(now, ManualClock) else ManualClock(now),
        calendar=CALENDAR,
        apply=apply,
        job_probe=probe,
        geteuid=lambda: euid,
        free_space=lambda _: free,
    )


def _refused(root: Path, client: FakeS3, **kwargs) -> str:
    with pytest.raises(ResyncRefused) as refused:
        _resync(root, client, **kwargs)
    message = str(refused.value)
    assert "\n" not in message
    return message


def _snapshot(root: Path) -> dict[str, bytes]:
    return {
        p.relative_to(root).as_posix(): p.read_bytes()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


def _entry(rel: str, sha: str, *, source: str = "compaction") -> bytes:
    line = {"partition": rel, "source": source, "sha256": sha, "rows": 100, "fetched_at": None}
    return (json.dumps(line, sort_keys=True) + "\n").encode()


def _gets(client: FakeS3) -> list[str]:
    return [kwargs["Key"] for name, kwargs in client.calls if name == "get_object"]


# -- 1. the classification -----------------------------------------------------------


def test_a_bucket_copy_equal_to_the_lake_has_nothing_to_do(tmp_path):
    laptop, client = _laptop(tmp_path)
    before = _snapshot(laptop)

    summary = _resync(laptop, client)

    assert summary.level
    assert summary.lines() == [
        "resync: nothing to do, the bucket's manifest.jsonl is this lake's or a prefix of it: "
        f"{TARGET}"
    ]
    assert _snapshot(laptop) == before


def test_a_bucket_copy_that_is_a_prefix_of_the_lake_has_nothing_to_do(tmp_path):
    laptop, client = _laptop(tmp_path)
    _laptop_sweep(laptop)

    assert _resync(laptop, client).level


def test_an_absent_bucket_copy_refuses_and_names_the_target(tmp_path):
    laptop, _ = _laptop(tmp_path)

    message = _refused(laptop, FakeS3())

    assert "holds no manifest.jsonl" in message
    assert "--target" in message
    assert message.endswith(str(TARGET))


@pytest.mark.parametrize("copy", [b"", b'{"partition": "chains/ticker=SPY'])
def test_a_bucket_copy_with_no_whole_entry_refuses_and_names_the_target(tmp_path, copy):
    laptop, client = _laptop(tmp_path)
    client.store(MANIFEST_KEY, copy)

    message = _refused(laptop, client)

    assert "carries no whole entry" in message
    assert message.endswith(str(TARGET))


def test_a_lake_that_is_a_prefix_of_the_bucket_copy_only_appends(tmp_path):
    laptop, client = _laptop(tmp_path)
    vm = _vm(tmp_path, client)
    files = _vm_session(vm, client)
    shared = len(manifest_path(laptop).read_bytes().splitlines())

    summary = _resync(laptop, client)

    assert not summary.level
    assert (summary.shared, summary.bucket_tail, summary.lake_tail) == (shared, 2, 0)
    assert summary.bucket_first == SPY_2
    assert summary.lake_first is None
    assert summary.downloads == [(rel, len(data)) for rel, data in sorted(files.items())]
    assert summary.deletions == []


def test_a_lake_with_its_own_tail_rewinds_it(tmp_path):
    laptop, _vm_root, client, files = _switched(tmp_path)
    bucket_quarantine = client.body(TARGET.key(QUARANTINE))
    before = _snapshot(laptop)

    summary = _resync(laptop, client)

    assert (summary.bucket_tail, summary.lake_tail) == (2, 2)
    assert (summary.bucket_first, summary.lake_first) == (SPY_2, BARS_2)
    planned = dict(summary.downloads)
    assert planned == {
        QUARANTINE: len(bucket_quarantine),
        **{rel: len(data) for rel, data in files.items()},
    }
    assert summary.deletions == [(BARS_2, "the next sweep regenerates it")]
    # A dry run writes nothing, and downloads nothing but the bucket's manifest.
    assert _snapshot(laptop) == before
    assert _gets(client) == [MANIFEST_KEY]


def test_a_rewind_names_both_tails_on_its_first_line(tmp_path):
    laptop, _vm_root, client, _files = _switched(tmp_path)
    shared = len(client.body(MANIFEST_KEY).splitlines()) - 2

    lines = _resync(laptop, client).lines()

    assert lines[0] == (
        f"resync: shared {shared} entries; bucket tail from {SPY_2!r} (2 entries); "
        f"this host's tail from {BARS_2!r} (2 entries)"
    )
    assert f"resync: delete {BARS_2} (the next sweep regenerates it)" in lines
    assert f"resync: download {SPY_2} (0.0 MB)" in lines


def test_a_hand_repair_refuses_and_names_the_first_upload(tmp_path):
    laptop, client = _laptop(tmp_path)
    # The bucket's copy keeps every entry the lake records but loses its first line, which a
    # hand repair of a damaged line leaves. Nothing in it is foreign.
    lines = client.body(MANIFEST_KEY).splitlines(keepends=True)
    client.store(MANIFEST_KEY, b"".join(lines[1:]))

    message = _refused(laptop, client)

    assert "hand repair" in message
    assert FIRST_UPLOAD_COMMAND in message


def test_a_rewind_that_shares_no_whole_line_refuses_before_any_download(tmp_path):
    laptop, _vm_root, client, _files = _switched(tmp_path)
    # Rot in the laptop's first line that still parses. The two copies then share no whole
    # line, so a rewind would cut the manifest to 0 bytes before writing the bucket's, and a
    # crash between the two would leave an empty manifest the next run refuses.
    first, rest = manifest_path(laptop).read_bytes().split(b"\n", 1)
    entry = json.loads(first)
    entry["rows"] += 1
    manifest_path(laptop).write_bytes(json.dumps(entry).encode() + b"\n" + rest)
    before = _snapshot(laptop)

    message = _refused(laptop, client, apply=True)

    assert "share no whole line" in message
    assert "first line" in message
    assert _gets(client) == [MANIFEST_KEY]
    assert _snapshot(laptop) == before


def _torn(rel: str) -> bytes:
    """The start of an entry for ``rel`` that a short write left with no newline."""
    return b'{"fetched_at": null, "partition": "' + rel.encode()


def test_a_fused_line_in_the_buckets_tail_refuses_naming_its_line(tmp_path):
    laptop, client = _laptop(tmp_path)
    copy = client.body(MANIFEST_KEY)
    line = len(copy.splitlines()) + 1
    # The VM's torn write fused with its next append, and a whole entry follows. Read with
    # the rule a reader applies, the entries past the fused line would be lost from the
    # plan while still reaching the lake's manifest.
    for rel in (SPY_2, QUOTES_2):
        data = f"vm {rel}".encode()
        client.store(TARGET.key(rel), data)
        copy += (_torn(SPY_2) if rel == SPY_2 else b"") + _entry(
            rel, hashlib.sha256(data).hexdigest()
        )
    client.store(MANIFEST_KEY, copy)

    message = _refused(laptop, client)

    assert f"line {line} of the bucket's manifest.jsonl" in message
    assert "by hand" in message
    assert message.endswith(str(TARGET))


def test_a_fused_line_in_this_lakes_tail_refuses_naming_its_line(tmp_path):
    laptop, _vm_root, client, _files = _switched(tmp_path)
    # The laptop's capture of a ticker the bucket never saw tore its manifest entry, and the
    # next append fused onto it. That file may be the only copy of the capture.
    rel = "chains/ticker=QQQ/date=2026-08-25.parquet"
    (laptop / rel).parent.mkdir(parents=True, exist_ok=True)
    (laptop / rel).write_bytes(b"the laptop's only copy")
    line = len(manifest_path(laptop).read_bytes().splitlines()) + 1
    with manifest_path(laptop).open("ab") as handle:
        handle.write(_torn(rel))
    _record(laptop, "bars/late.parquet", b"late", source="sweep")

    message = _refused(laptop, client)

    assert f"line {line} of {manifest_path(laptop)}" in message
    assert "by hand" in message


@pytest.mark.parametrize("partition", [5, None, True, ["a"]])
def test_a_bucket_entry_whose_partition_is_not_a_string_refuses_naming_its_line(
    tmp_path, partition
):
    laptop, client = _laptop(tmp_path)
    copy = client.body(MANIFEST_KEY)
    line = len(copy.splitlines()) + 1
    client.store(
        MANIFEST_KEY,
        copy
        + _entry(SPY_2, "0" * 64).replace(
            json.dumps(SPY_2).encode(), json.dumps(partition).encode()
        ),
    )

    message = _refused(laptop, client)

    assert f"line {line} of the bucket's manifest.jsonl" in message


def test_a_damaged_first_line_in_the_buckets_copy_names_that_line(tmp_path):
    laptop, client = _laptop(tmp_path)
    lines = client.body(MANIFEST_KEY).splitlines(keepends=True)
    client.store(MANIFEST_KEY, b"not json\n" + b"".join(lines[1:]))

    message = _refused(laptop, client)

    assert "line 1 of the bucket's manifest.jsonl" in message
    assert "carries no whole entry" not in message


class _ChangingS3(FakeS3):
    """A fake bucket whose ``manifest.jsonl`` gains a line between its HEAD and its GET."""

    def get_object(self, **kwargs) -> dict:
        if kwargs["Key"] == MANIFEST_KEY:
            self.store(MANIFEST_KEY, self.body(MANIFEST_KEY) + _entry(SPY_2, "0" * 64))
        return super().get_object(**kwargs)


def test_a_copy_that_changed_between_its_head_and_its_get_refuses(tmp_path):
    laptop, client = _laptop(tmp_path)
    changing = _ChangingS3()
    changing.objects = client.objects

    message = _refused(laptop, changing)

    assert "changed between its HEAD and its GET" in message


def test_a_bucket_failure_on_the_read_raises_for_main_to_name(tmp_path):
    laptop, client = _laptop(tmp_path)
    client.fail_with = client_error("AccessDenied", "HeadObject", 403)

    with pytest.raises(Exception) as raised:
        _resync(laptop, client)

    assert bucket._one_line(raised.value, TARGET) is not None


# -- 2. the plan -----------------------------------------------------------------------


def test_a_file_already_on_disk_with_the_buckets_sha_is_not_downloaded(tmp_path):
    laptop, vm, client, files = _switched(tmp_path)
    (laptop / SPY_2).parent.mkdir(parents=True, exist_ok=True)
    (laptop / SPY_2).write_bytes(files[SPY_2])

    planned = dict(_resync(laptop, client).downloads)

    assert SPY_2 not in planned
    assert QUOTES_2 in planned


def test_a_segment_its_compacted_partition_supersedes_is_not_downloaded(tmp_path):
    laptop, client = _laptop(tmp_path)
    vm = _vm(tmp_path, client)
    # The VM's capture recorded a segment, then compaction sealed its partition.
    _record(vm, SEGMENT_2, b"segment bytes", source="capture")
    (vm / SEGMENT_2).unlink()
    _record(vm, SPY_2, b"vm chains 2026-08-25")
    nightly_upload(vm, TARGET, client=client, clock=ManualClock(TUESDAY_19), calendar=CALENDAR)

    planned = dict(_resync(laptop, client).downloads)

    assert SEGMENT_2 not in planned
    assert SPY_2 in planned


def test_a_segment_whose_day_was_never_compacted_is_downloaded(tmp_path):
    laptop, client = _laptop(tmp_path)
    vm = _vm(tmp_path, client)
    _record(vm, SEGMENT_2, b"segment bytes", source="capture")
    nightly_upload(vm, TARGET, client=client, clock=ManualClock(TUESDAY_19), calendar=CALENDAR)

    assert dict(_resync(laptop, client).downloads) == {SEGMENT_2: len(b"segment bytes")}


def test_a_designed_absence_in_the_buckets_ledger_is_not_downloaded(tmp_path):
    from lake.trimmed import append_trimmed, trim_line

    laptop, client = _laptop(tmp_path)
    vm = _vm(tmp_path, client)
    sha = _record(vm, SPY_2, b"vm chains 2026-08-25")
    _record(vm, QUOTES_2, b"vm quotes 2026-08-25")
    nightly_upload(vm, TARGET, client=client, clock=ManualClock(TUESDAY_19), calendar=CALENDAR)
    # The VM's trim then drops the chains partition, recording it in its trimmed ledger.
    line = trim_line(SPY_2, sha256=sha, version_id="v1", verified_at="s", trimmed_at="s")
    append_trimmed(vm, line, source="test-trim", fetched_at=None)
    (vm / SPY_2).unlink()
    nightly_upload(vm, TARGET, client=client, clock=ManualClock(TUESDAY_20), calendar=CALENDAR)

    planned = dict(_resync(laptop, client).downloads)

    assert SPY_2 not in planned
    assert QUOTES_2 in planned
    assert "trimmed.jsonl" in planned


def test_a_designed_absence_this_lake_holds_on_disk_refuses(tmp_path):
    from lake.trimmed import append_trimmed, trim_line

    laptop, client = _laptop(tmp_path)
    vm = _vm(tmp_path, client)
    sha = _record(vm, SPY_2, b"vm chains 2026-08-25")
    nightly_upload(vm, TARGET, client=client, clock=ManualClock(TUESDAY_19), calendar=CALENDAR)
    line = trim_line(SPY_2, sha256=sha, version_id="v1", verified_at="s", trimmed_at="s")
    append_trimmed(vm, line, source="test-trim", fetched_at=None)
    (vm / SPY_2).unlink()
    nightly_upload(vm, TARGET, client=client, clock=ManualClock(TUESDAY_20), calendar=CALENDAR)
    # The shadow laptop sealed the same session with its own bytes. Left on disk, they would
    # sit under the bucket's entry for the VM's bytes, which the scrub reads as a mismatch.
    _record(laptop, SPY_2, b"laptop chains 2026-08-25")

    message = _refused(laptop, client)

    assert repr(SPY_2) in message
    assert "removed on purpose" in message
    assert "Move each file out of the lake by hand" in message
    (laptop / SPY_2).unlink()
    assert SPY_2 not in dict(_resync(laptop, client).downloads)


def test_a_buckets_trimmed_ledger_that_is_missing_refuses(tmp_path):
    laptop, client = _laptop(tmp_path)
    copy = client.body(MANIFEST_KEY) + _entry("trimmed.jsonl", "0" * 64, source="trim")
    client.store(MANIFEST_KEY, copy)

    message = _refused(laptop, client)

    assert "holds no current trimmed.jsonl" in message
    assert message.endswith("The resync changed nothing")


def test_an_unsafe_key_in_the_buckets_tail_refuses(tmp_path):
    laptop, client = _laptop(tmp_path)
    copy = client.body(MANIFEST_KEY) + _entry("../outside.parquet", "0" * 64)
    client.store(MANIFEST_KEY, copy)

    message = _refused(laptop, client)

    assert "'../outside.parquet'" in message
    assert "outside the lake" in message
    assert not (tmp_path / "outside.parquet").exists()


def test_a_download_beside_a_path_that_differs_only_by_case_refuses(tmp_path):
    laptop, client = _laptop(tmp_path)
    # Stored straight into the bucket, since this test's own filesystem may ignore case.
    copy = client.body(MANIFEST_KEY)
    for rel, data in ((SPY_2, b"upper"), (SPY_2.replace("SPY", "spy"), b"lower")):
        client.store(TARGET.key(rel), data)
        copy += _entry(rel, hashlib.sha256(data).hexdigest())
    client.store(MANIFEST_KEY, copy)

    message = _refused(laptop, client)

    assert "differs only by case" in message


def test_a_download_and_a_deletion_that_differ_only_by_case_refuse(tmp_path):
    laptop, _vm_root, client, _files = _switched(tmp_path)
    # The laptop's tail names BARS_2, which the resync deletes. On a filesystem that ignores
    # case, a download of the same path in another case is that file, so the commit would
    # rename the download in and then unlink it.
    other = BARS_2.replace("ticker=SPY", "ticker=spy")
    data = b"vm bars in another case"
    client.store(TARGET.key(other), data)
    client.store(
        MANIFEST_KEY,
        client.body(MANIFEST_KEY) + _entry(other, hashlib.sha256(data).hexdigest()),
    )

    message = _refused(laptop, client)

    assert "differs only by case" in message
    assert repr(other) in message


def test_a_download_beside_a_path_this_lakes_tail_names_in_another_case_refuses(tmp_path):
    laptop, _vm_root, client, _files = _switched(tmp_path)
    mine = "reference/other_table.parquet"
    _record(laptop, mine, b"laptop only", source="reference")
    (laptop / mine).unlink()
    theirs = "reference/Other_table.parquet"
    data = b"vm table"
    client.store(TARGET.key(theirs), data)
    client.store(
        MANIFEST_KEY,
        client.body(MANIFEST_KEY) + _entry(theirs, hashlib.sha256(data).hexdigest()),
    )

    message = _refused(laptop, client)

    assert "differs only by case" in message
    assert repr(theirs) in message


def test_a_covered_segment_only_this_lake_names_is_deleted(tmp_path):
    laptop, vm, client, _files = _switched(tmp_path)
    _record(laptop, SEGMENT_2, b"laptop segment", source="capture")

    summary = _resync(laptop, client)

    assert (SEGMENT_2, f"covered by {SPY_2}") in summary.deletions


def test_a_file_the_next_upload_replaces_is_named(tmp_path):
    laptop, vm, client, _files = _switched(tmp_path)
    metadata = "journal/metadata.json"
    client.store(TARGET.key(metadata), b'{"stamped_at": "the VM\'s"}\n')
    (laptop / metadata).parent.mkdir(parents=True, exist_ok=True)
    (laptop / metadata).write_bytes(b"{}\n")
    # A file whose bucket object has its size is left alone, and so is one with no object.
    (laptop / "journal" / "same.json").write_bytes(b"12345")
    client.store(TARGET.key("journal/same.json"), b"abcde")
    (laptop / "journal" / "new.json").write_bytes(b"new")

    summary = _resync(laptop, client)

    assert summary.unrecorded == [metadata]
    assert f"resync: unrecorded, the next upload replaces it: {metadata}" in summary.lines()


# -- 3. the refusals on this lake's own tail ---------------------------------------------


@pytest.mark.parametrize(
    "rel",
    [
        pytest.param("chains/ticker=QQQ/date=2026-08-25.parquet", id="chains"),
        pytest.param("quotes/ticker=QQQ/date=2026-08-25.parquet", id="quotes"),
        pytest.param(
            "journal/date=2026-08-25/surface=chains/ticker=QQQ/seg-20260825T1330-41.arrows",
            id="segment",
        ),
    ],
)
def test_a_file_only_this_lake_holds_may_be_the_only_copy_and_refuses(tmp_path, rel):
    laptop, vm, client, _files = _switched(tmp_path)
    _record(laptop, rel, b"the laptop's only copy", source="capture")

    message = _refused(laptop, client)

    assert "only copy" in message
    assert repr(rel) in message
    # Moving the file out of the lake clears it.
    (laptop / rel).unlink()
    assert _resync(laptop, client).downloads


def test_a_segment_the_buckets_segments_share_a_day_with_is_not_the_only_copy(tmp_path):
    laptop, client = _laptop(tmp_path)
    vm = _vm(tmp_path, client)
    _record(vm, SEGMENT_2, b"vm segment", source="capture")
    nightly_upload(vm, TARGET, client=client, clock=ManualClock(TUESDAY_19), calendar=CALENDAR)
    mine = SEGMENT_2.replace("-41.arrows", "-77.arrows")
    _record(laptop, mine, b"laptop segment", source="capture")

    message = _refused(laptop, client)

    assert "only copy" not in message
    assert "does not delete" in message
    assert repr(mine) in message


@pytest.mark.parametrize(
    "rel",
    [
        pytest.param("chains/ticker=QQQ/date=2026-08-25.parquet", id="chains"),
        pytest.param(SEGMENT_2.replace("-41.arrows", "-77.arrows"), id="segment"),
        pytest.param("reference/other_table.parquet", id="other"),
    ],
)
def test_a_file_only_this_lake_holds_refuses_whatever_its_bytes(tmp_path, rel):
    laptop, client = _laptop(tmp_path)
    vm = _vm(tmp_path, client)
    # The VM's segment of the same day keeps the laptop's segment from being the only copy.
    _record(vm, SEGMENT_2, b"vm segment", source="capture")
    nightly_upload(vm, TARGET, client=client, clock=ManualClock(TUESDAY_19), calendar=CALENDAR)
    _record(laptop, rel, b"recorded", source="capture")
    # Bytes that no longer match the laptop's entry. Left on disk with no entry, the file
    # would be an orphan to the scrub, and a segment would be merged unchecked.
    (laptop / rel).write_bytes(b"replaced since")

    message = _refused(laptop, client)

    assert repr(rel) in message
    assert "Move each file out of the lake by hand" in message


def test_a_capture_spans_rewrite_in_this_lakes_tail_refuses(tmp_path):
    laptop, vm, client, _files = _switched(tmp_path)
    _record(vm, SPANS, b"the vm's spans", source="reference")
    nightly_upload(vm, TARGET, client=client, clock=ManualClock(TUESDAY_20), calendar=CALENDAR)
    _record(laptop, SPANS, b"the laptop's spans", source="reference")

    message = _refused(laptop, client)

    assert "human decision" in message
    assert repr(SPANS) in message
    # Once the laptop's file is moved aside, the bucket's version replaces what is there.
    (laptop / SPANS).write_bytes(b"moved aside and replaced")
    assert SPANS in dict(_resync(laptop, client).downloads)


def test_a_capture_spans_file_only_this_lake_names_is_cleared_by_moving_it_out(tmp_path):
    laptop, vm, client, _files = _switched(tmp_path)
    _record(laptop, SPANS, b"the laptop's spans", source="reference")

    assert "human decision" in _refused(laptop, client)
    # The bucket names no spans file, so other bytes left there would be unrecorded.
    (laptop / SPANS).write_bytes(b"replaced in place")
    assert repr(SPANS) in _refused(laptop, client)
    (laptop / SPANS).unlink()
    assert _resync(laptop, client).downloads


def test_a_signoff_in_this_lakes_tail_refuses_and_a_battery_verdict_does_not(tmp_path):
    laptop, vm, client, _files = _switched(tmp_path)
    assert QUARANTINE in dict(_resync(laptop, client).downloads)
    quarantine = laptop / QUARANTINE
    with quarantine.open("a") as handle:
        handle.write(json.dumps({"partition": SPY_1, "check": "row_count", "verdict": "x"}))
        handle.write("\n")
    append_manifest(
        laptop,
        partition=QUARANTINE,
        source="signoff",
        sha256=sha256_file(quarantine),
        rows=100,
        fetched_at=None,
    )

    message = _refused(laptop, client)

    assert "human decision" in message
    assert repr(QUARANTINE) in message


def test_any_other_file_only_this_lake_holds_refuses(tmp_path):
    laptop, vm, client, _files = _switched(tmp_path)
    rel = "reference/other_table.parquet"
    _record(laptop, rel, b"laptop only", source="reference")

    message = _refused(laptop, client)

    assert "does not delete" in message
    assert repr(rel) in message


def test_a_schema_version_ledger_in_this_lakes_tail_warns_and_does_not_refuse(tmp_path):
    laptop, vm, client, _files = _switched(tmp_path)
    _record(vm, SCHEMA_LEDGER, b"vm ledger", source="reference")
    nightly_upload(vm, TARGET, client=client, clock=ManualClock(TUESDAY_20), calendar=CALENDAR)
    _record(laptop, SCHEMA_LEDGER, b"laptop ledger", source="reference")

    summary = _resync(laptop, client)

    assert SCHEMA_LEDGER in dict(summary.downloads)
    (warning,) = summary.warnings
    assert SCHEMA_LEDGER in warning
    assert "python -m lake.schema_versions" in warning


# -- 4. the checks before any download -------------------------------------------------


def test_a_download_the_bucket_no_longer_lists_refuses(tmp_path):
    laptop, vm, client, _files = _switched(tmp_path)
    del client.objects[TARGET.key(QUOTES_2)]

    message = _refused(laptop, client)

    assert repr(QUOTES_2) in message
    assert "no current version" in message


def test_an_object_newer_than_the_buckets_manifest_refuses_naming_both_causes(tmp_path):
    laptop, vm, client, _files = _switched(tmp_path)
    client.store(TARGET.key(QUOTES_2), b"a later upload's bytes")

    message = _refused(laptop, client)

    assert repr(QUOTES_2) in message
    assert "stopped before its manifest.jsonl PUT, or is still running" in message
    assert _gets(client) == [MANIFEST_KEY]


def test_a_volume_short_of_the_journal_reserve_refuses(tmp_path):
    laptop, vm, client, files = _switched(tmp_path)

    message = _refused(laptop, client, free=10)

    assert "journal reserve" in message
    assert "The resync changed nothing" in message


# -- 5. the guards and the command -------------------------------------------------------


def test_a_lake_root_with_no_manifest_refuses(tmp_path):
    _, client = _laptop(tmp_path)
    client.calls.clear()
    empty = tmp_path / "empty"
    empty.mkdir()

    message = _refused(empty, client)

    assert "holds no manifest.jsonl" in message
    assert not (empty / "manifest.jsonl").exists()
    assert client.calls == []


def test_root_refuses(tmp_path):
    laptop, vm, client, _files = _switched(tmp_path)

    message = _refused(laptop, client, euid=0)

    assert "as root" in message


def test_the_sunday_window_refuses(tmp_path):
    laptop, vm, client, _files = _switched(tmp_path)

    assert "19:55 to 23:30" in _refused(laptop, client, now=SUNDAY_20)


def test_a_session_refuses(tmp_path):
    laptop, vm, client, _files = _switched(tmp_path)

    assert "ahead of the next session's capture start" in _refused(laptop, client, now=TUESDAY_10)


def test_the_run_names_the_moment_it_must_stop_by(tmp_path):
    laptop, vm, client, _files = _switched(tmp_path)

    summary = _resync(laptop, client)

    assert summary.stop_by is not None
    assert summary.stop_by.isoformat() == WEDNESDAY_BOUND


def _config(tmp_path: Path, lake_root: Path, *, role: str | None = None) -> Path:
    config = write_config(tmp_path, lake_root, role=role)
    config.write_text(config.read_text() + KEYS)
    return config


def _main(config: Path, client: FakeS3, monkeypatch, *argv: str, now=TUESDAY_20) -> int:
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    monkeypatch.setattr(bucket.os, "geteuid", lambda: OWNER)
    monkeypatch.setattr(bucket, "default_job_probe", lambda: _idle)
    return bucket.main(
        ["resync", "--config", str(config), "--target", str(TARGET), *argv],
        clock=ManualClock(now),
        calendar=CALENDAR,
    )


def test_the_dry_run_prints_one_line_per_fact_and_writes_nothing(tmp_path, monkeypatch, capsys):
    laptop, vm, client, files = _switched(tmp_path)
    config = _config(tmp_path, laptop)
    before = _snapshot(laptop)

    assert _main(config, client, monkeypatch) == 0

    out = capsys.readouterr().out.splitlines()
    assert out[0].startswith("resync: shared ")
    assert f"resync: delete {BARS_2} (the next sweep regenerates it)" in out
    assert out[-1] == (
        "resync: dry run: 3 download(s), 0.0 MB, 1 deletion(s), must stop by "
        f"{WEDNESDAY_BOUND}. Run again with --apply"
    )
    assert _snapshot(laptop) == before


def test_a_refusal_exits_2_with_one_line(tmp_path, monkeypatch, capsys):
    laptop, vm, client, _files = _switched(tmp_path)
    config = _config(tmp_path, laptop)

    with pytest.raises(SystemExit) as exited:
        _main(config, client, monkeypatch, now=SUNDAY_20)

    assert exited.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    (line,) = captured.err.splitlines()
    assert line.startswith("resync: the resync does not run on Sunday")


def test_a_bucket_failure_exits_2_with_one_line(tmp_path, monkeypatch, capsys):
    laptop, vm, client, _files = _switched(tmp_path)
    config = _config(tmp_path, laptop)
    client.fail_with = client_error("AccessDenied", "HeadObject", 403)

    with pytest.raises(SystemExit) as exited:
        _main(config, client, monkeypatch)

    assert exited.value.code == 2
    (line,) = capsys.readouterr().err.splitlines()
    assert line.startswith("resync: the bucket refused the request (AccessDenied)")


# -- 6. --apply ----------------------------------------------------------------------------


class Crash(Exception):
    """A process death injected between two steps. No branch of the command catches it."""


class _HookedS3(FakeS3):
    """A fake bucket that runs ``hook`` before each ``GetObject`` of a data file.

    It is not ``FakeS3``'s own ``on_get``, which runs on every ``GetObject``, the manifest's
    included.
    """

    def __init__(self, source: FakeS3, hook) -> None:
        super().__init__()
        self.objects = source.objects
        self.hook = hook

    def get_object(self, **kwargs) -> dict:
        if kwargs["Key"] != MANIFEST_KEY:
            self.hook(kwargs["Key"])
        return super().get_object(**kwargs)


def _strays(root: Path) -> list[str]:
    """Every temp file left anywhere under the lake root."""
    return sorted(p.relative_to(root).as_posix() for p in root.rglob("*") if ".tmp-" in p.name)


def _crash(*args) -> None:
    raise Crash


def test_apply_brings_the_lake_level_and_keeps_the_manifests_inode(tmp_path):
    from lake.manifest import scrub

    laptop, _vm_root, client, files = _switched(tmp_path)
    inode = manifest_path(laptop).stat().st_ino
    expected = client.body(MANIFEST_KEY)

    summary = _resync(laptop, client, apply=True)

    assert summary.applied
    assert manifest_path(laptop).read_bytes() == expected
    assert manifest_path(laptop).stat().st_ino == inode
    assert summary.entries == len(expected.splitlines())
    for rel, data in files.items():
        assert (laptop / rel).read_bytes() == data
    assert (laptop / QUARANTINE).read_bytes() == client.body(TARGET.key(QUARANTINE))
    assert not (laptop / BARS_2).exists()
    assert _strays(laptop) == []
    assert scrub(laptop).ok
    # A second run finds the lake level.
    assert _resync(laptop, client, apply=True).level


def test_apply_on_a_lake_that_only_appends_cuts_a_torn_last_line_first(tmp_path):
    laptop, client = _laptop(tmp_path)
    vm = _vm(tmp_path, client)
    _vm_session(vm, client)
    # A torn write left part of the bucket's next line on the end of the laptop's manifest.
    expected = client.body(MANIFEST_KEY)
    lake_raw = manifest_path(laptop).read_bytes()
    with manifest_path(laptop).open("ab") as handle:
        handle.write(expected[len(lake_raw) : len(lake_raw) + 20])

    summary = _resync(laptop, client, apply=True)

    assert manifest_path(laptop).read_bytes() == expected
    # The torn fragment is the start of the bucket's first tail entry, so the tail counts
    # from that entry's own line rather than from the line after the fragment.
    assert (summary.bucket_tail, summary.bucket_first) == (2, SPY_2)


def test_the_dry_run_never_asks_the_job_probe(tmp_path):
    laptop, _vm_root, client, _files = _switched(tmp_path)

    def probe(label: str) -> bool:
        pytest.fail("the dry run asked whether a job was executing")

    assert _resync(laptop, client, probe=probe).downloads


@pytest.mark.parametrize(
    "label",
    ["com.marketlake.daemon", "com.marketlake.eod-sweep", "com.marketlake.sunday"],
)
def test_an_executing_job_refuses_apply_before_any_request(tmp_path, label):
    laptop, _vm_root, client, _files = _switched(tmp_path)
    before = _snapshot(laptop)

    message = _refused(laptop, client, apply=True, probe=lambda asked: asked == label)

    assert label in message
    assert "changed nothing" in message
    assert client.calls == []
    assert _snapshot(laptop) == before


def test_a_job_that_starts_during_the_downloads_refuses_at_the_commit(tmp_path):
    laptop, _vm_root, client, _files = _switched(tmp_path)
    before = _snapshot(laptop)
    asked: list[str] = []

    def probe(label: str) -> bool:
        # Idle at the start and before each of the three downloads, then the daemon is up at
        # the commit.
        asked.append(label)
        return len(asked) > 4 * 3

    message = _refused(laptop, client, apply=True, probe=probe)

    assert "com.marketlake.daemon is executing" in message
    assert _snapshot(laptop) == before
    assert _strays(laptop) == []


def test_a_lake_writer_during_the_downloads_refuses_at_the_commit(tmp_path):
    laptop, _vm_root, client, _files = _switched(tmp_path)

    def sweep(_key: str) -> None:
        if not (laptop / "bars/late.parquet").exists():
            _record(laptop, "bars/late.parquet", b"late", source="sweep")

    message = _refused(laptop, _HookedS3(client, sweep), apply=True)

    assert "the lake's manifest.jsonl changed while the resync downloaded" in message
    assert (laptop / BARS_2).exists()
    assert _strays(laptop) == []


def test_an_upload_to_the_bucket_during_the_downloads_refuses_at_the_commit(tmp_path):
    laptop, _vm_root, client, _files = _switched(tmp_path)
    before = _snapshot(laptop)

    def upload(_key: str) -> None:
        if not client.body(MANIFEST_KEY).endswith(b"late\n"):
            client.store(MANIFEST_KEY, client.body(MANIFEST_KEY) + b"late\n")

    message = _refused(laptop, _HookedS3(client, upload), apply=True)

    assert "the bucket's manifest.jsonl changed while the resync downloaded" in message
    assert _snapshot(laptop) == before
    assert _strays(laptop) == []


def test_a_download_that_does_not_hash_to_its_entry_refuses_and_keeps_no_temp(tmp_path):
    laptop, _vm_root, client, _files = _switched(tmp_path)
    before = _snapshot(laptop)

    def replace(key: str) -> None:
        if key == TARGET.key(SPY_2):
            client.store(key, b"replaced after the check")

    message = _refused(laptop, _HookedS3(client, replace), apply=True)

    assert repr(SPY_2) in message
    assert "does not hash" in message
    assert _snapshot(laptop) == before
    assert _strays(laptop) == []


class _BrokenBody:
    """A response body that gives up a first chunk, then fails as a dropped read would."""

    def __init__(self) -> None:
        self.reads = 0

    def read(self, _size: int) -> bytes:
        self.reads += 1
        if self.reads == 1:
            return b"the first chunk"
        raise client_error("InternalError", "GetObject", 500)

    def close(self) -> None:
        pass


class _FailingGetS3(FakeS3):
    """A fake bucket whose ``GetObject`` of one key fails, on the request or in its body."""

    def __init__(self, source: FakeS3, key: str, *, in_body: bool) -> None:
        super().__init__()
        self.objects = source.objects
        self.key = key
        self.in_body = in_body

    def get_object(self, **kwargs) -> dict:
        if kwargs["Key"] != self.key:
            return super().get_object(**kwargs)
        if self.in_body:
            return {"Body": _BrokenBody()}
        raise client_error("InternalError", "GetObject", 500)


@pytest.mark.parametrize("in_body", [False, True], ids=["request", "body"])
def test_a_download_that_fails_leaves_no_temp_file(tmp_path, in_body):
    laptop, _vm_root, client, _files = _switched(tmp_path)
    before = _snapshot(laptop)
    failing = _FailingGetS3(client, TARGET.key(SPY_2), in_body=in_body)

    with pytest.raises(bucket.BucketReadError) as raised:
        _resync(laptop, failing, apply=True)

    assert bucket._one_line(raised.value, TARGET) is not None
    assert _strays(laptop) == []
    assert _snapshot(laptop) == before


def test_apply_flushes_the_manifest_and_each_download_past_the_drive_cache(tmp_path, monkeypatch):
    import os
    import types

    laptop, _vm_root, client, files = _switched(tmp_path)
    full = object()
    flushed: set[int] = set()

    def fcntl(fd, command, *args):
        assert command is full
        flushed.add(os.fstat(fd).st_ino)
        return 0

    monkeypatch.setattr(bucket, "F_FULLFSYNC", full)
    monkeypatch.setattr(bucket, "fcntl", types.SimpleNamespace(fcntl=fcntl))

    assert _resync(laptop, client, apply=True).applied

    expected = {manifest_path(laptop).stat().st_ino, (laptop / QUARANTINE).stat().st_ino}
    expected |= {(laptop / rel).stat().st_ino for rel in files}
    assert expected <= flushed


def test_apply_flushes_the_parent_of_each_directory_a_download_creates(tmp_path, monkeypatch):
    laptop, client = _laptop(tmp_path)
    vm = _vm(tmp_path, client)
    _record(vm, SEGMENT_2, b"segment bytes", source="capture")
    nightly_upload(vm, TARGET, client=client, clock=ManualClock(TUESDAY_19), calendar=CALENDAR)
    created = [
        parent
        for parent in (laptop / SEGMENT_2).parents
        if parent.is_relative_to(laptop) and parent != laptop and not parent.exists()
    ]
    assert created
    flushed: list[Path] = []
    real = bucket._fsync_path

    def record(path: Path) -> None:
        flushed.append(Path(path))
        real(path)

    monkeypatch.setattr(bucket, "_fsync_path", record)

    assert _resync(laptop, client, apply=True).applied

    assert (laptop / SEGMENT_2).read_bytes() == b"segment bytes"
    for directory in created:
        assert directory.parent in flushed


def test_a_session_reached_during_the_downloads_stops_and_discards_them(tmp_path):
    laptop, _vm_root, client, _files = _switched(tmp_path)
    before = _snapshot(laptop)
    clock = ManualClock(TUESDAY_20)

    def late(_key: str) -> None:
        clock.advance(13 * 3600)

    message = _refused(laptop, _HookedS3(client, late), apply=True, now=clock)

    assert "ahead of the next session's capture start" in message
    assert _snapshot(laptop) == before
    assert _strays(laptop) == []


def test_a_leftover_temp_beside_a_target_is_removed_and_named(tmp_path):
    laptop, _vm_root, client, _files = _switched(tmp_path)
    stale = laptop / f"{SPY_2}.tmp-99999"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_bytes(b"a crashed run's download")

    summary = _resync(laptop, client, apply=True)

    assert summary.temps_removed == [f"{SPY_2}.tmp-99999"]
    assert f"resync: removed a leftover temp file: {SPY_2}.tmp-99999" in summary.lines()
    assert _strays(laptop) == []


def test_a_crash_before_the_manifest_rewrite_is_finished_by_the_next_run(tmp_path, monkeypatch):
    laptop, _vm_root, client, files = _switched(tmp_path)
    lake_raw = manifest_path(laptop).read_bytes()
    expected = client.body(MANIFEST_KEY)

    with monkeypatch.context() as patched:
        patched.setattr(bucket, "_rewrite_manifest", _crash)
        with pytest.raises(Crash):
            _resync(laptop, client, apply=True)
    # The files moved in and the bars partition went, and the manifest is still the lake's.
    assert manifest_path(laptop).read_bytes() == lake_raw
    assert (laptop / SPY_2).read_bytes() == files[SPY_2]
    assert not (laptop / BARS_2).exists()

    summary = _resync(laptop, client, apply=True)

    assert summary.applied
    assert summary.downloads == []
    assert manifest_path(laptop).read_bytes() == expected


@pytest.mark.parametrize("past", [0, 7, 200])
def test_a_crash_part_way_through_the_manifest_rewrite_is_finished_by_the_next_run(
    tmp_path, monkeypatch, past
):
    laptop, _vm_root, client, _files = _switched(tmp_path)
    expected = client.body(MANIFEST_KEY)
    with monkeypatch.context() as patched:
        patched.setattr(bucket, "_rewrite_manifest", _crash)
        with pytest.raises(Crash):
            _resync(laptop, client, apply=True)
    # What a crash after the truncate leaves: the shared bytes and part of the bucket's tail,
    # cut mid-line, written in place.
    shared = len(b"".join(expected.splitlines(keepends=True)[:-2]))
    with manifest_path(laptop).open("r+b") as handle:
        handle.truncate(shared)
        handle.seek(shared)
        handle.write(expected[shared : shared + past])

    summary = _resync(laptop, client, apply=True)

    assert summary.applied
    assert summary.lake_tail == 0
    assert manifest_path(laptop).read_bytes() == expected


def test_the_rewrite_keeps_the_lock_held_by_another_descriptor(tmp_path):
    import fcntl
    import os

    laptop, _vm_root, client, _files = _switched(tmp_path)
    expected = client.body(MANIFEST_KEY)
    keep = len(b"".join(expected.splitlines(keepends=True)[:-2]))
    holder = os.open(manifest_path(laptop), os.O_RDONLY)
    try:
        fcntl.flock(holder, fcntl.LOCK_EX)
        bucket._rewrite_manifest(laptop, keep, expected[keep:], expected)
        other = os.open(manifest_path(laptop), os.O_RDONLY)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(other)
    finally:
        os.close(holder)
    assert manifest_path(laptop).read_bytes() == expected


def test_the_command_applies_and_prints_its_verdict_lines(tmp_path, monkeypatch, capsys):
    laptop, _vm_root, client, _files = _switched(tmp_path)
    config = _config(tmp_path, laptop)

    assert _main(config, client, monkeypatch, "--apply") == 0

    out = capsys.readouterr().out.splitlines()
    entries = len(client.body(MANIFEST_KEY).splitlines())
    assert (
        "resync: applied: 3 download(s), 0.0 MB, 1 deletion(s), and manifest.jsonl now holds "
        f"the bucket's {entries} entries"
    ) in out
    # The roster check and the schema-version check report after the commit, and a roster
    # that cannot be read is a warning, since the lake is already written.
    assert any(line.startswith("resync: warning: tickers file not found") for line in out)
    assert any("schema version" in line for line in out)
    assert out[-1].startswith(f"resync: backup_target is {tmp_path / 'ssd'}, not {TARGET}")
    assert manifest_path(laptop).read_bytes() == client.body(MANIFEST_KEY)


def test_the_command_names_no_backup_target_that_is_already_the_bucket(
    tmp_path, monkeypatch, capsys
):
    laptop, _vm_root, client, _files = _switched(tmp_path)
    config = _config(tmp_path, laptop)
    text = config.read_text()
    config.write_text(
        text.replace(f"backup_target: {tmp_path / 'ssd'}", f"backup_target: {TARGET}")
    )

    assert _main(config, client, monkeypatch, "--apply") == 0

    assert "backup_target is" not in capsys.readouterr().out


# -- 7. the probe a systemd host asks ---------------------------------------------------


@pytest.mark.parametrize(
    ("stdout", "code", "executing"),
    [
        ("activating\n", 0, True),
        ("active\n", 0, True),
        ("deactivating\n", 0, True),
        ("reloading\n", 0, True),
        ("inactive\n", 0, False),
        ("failed\n", 0, False),
        ("", 1, True),
    ],
)
def test_the_systemd_probe_reads_the_active_state(monkeypatch, stdout, code, executing):
    import subprocess

    from lake import control_plane

    seen: list[list[str]] = []

    def run(args, **kwargs):
        seen.append(list(args))
        return subprocess.CompletedProcess(args, code, stdout=stdout, stderr="")

    monkeypatch.setattr(subprocess, "run", run)

    assert control_plane.systemctl_executing_probe("com.marketlake.eod-sweep") is executing
    assert seen == [
        ["systemctl", "show", "-p", "ActiveState", "--value", "com.marketlake.eod-sweep.service"]
    ]


@pytest.mark.parametrize(
    ("stdout", "code", "executing"),
    [
        ("\tstate = running\n", 0, True),
        ("\tstate = waiting\n", 0, False),
        ("", 113, False),
        ("", 1, True),
        ("", 5, True),
    ],
)
def test_the_launchd_probe_reads_a_failed_call_as_executing(monkeypatch, stdout, code, executing):
    import subprocess

    from lake import control_plane

    seen: list[list[str]] = []

    def run(args, **kwargs):
        seen.append(list(args))
        return subprocess.CompletedProcess(args, code, stdout=stdout, stderr="")

    monkeypatch.setattr(subprocess, "run", run)

    assert control_plane.launchctl_executing_probe("com.marketlake.eod-sweep") is executing
    assert seen == [
        ["launchctl", "print", f"{control_plane.LAUNCHD_DOMAIN}/com.marketlake.eod-sweep"]
    ]
    # The self-check's probe keeps reading any failed call as not running.
    seen.clear()
    assert control_plane.launchctl_probe("com.marketlake.eod-sweep") is (code == 0 and executing)


@pytest.mark.parametrize("macos", [True, False])
def test_the_job_probe_follows_the_host(monkeypatch, macos):
    from lake import control_plane

    monkeypatch.setattr(control_plane, "is_macos", lambda: macos)

    expected = (
        control_plane.launchctl_executing_probe
        if macos
        else control_plane.systemctl_executing_probe
    )
    assert bucket.default_job_probe() is expected


def test_the_job_probe_is_exported():
    assert {"JobProbe", "default_job_probe"} <= set(bucket.__all__)


# -- 8. both directions, end to end ------------------------------------------------------

WEDNESDAY_19 = datetime(2026, 8, 26, 19, 0, tzinfo=MARKET_TZ)
WEDNESDAY_20 = datetime(2026, 8, 26, 20, 0, tzinfo=MARKET_TZ)
THURSDAY_19 = datetime(2026, 8, 27, 19, 0, tzinfo=MARKET_TZ)
SPY_3 = "chains/ticker=SPY/date=2026-08-26.parquet"
SPY_4 = "chains/ticker=SPY/date=2026-08-27.parquet"
SEGMENT_3 = "journal/date=2026-08-26/surface=chains/ticker=SPY/seg-20260826T1330-52.arrows"


def _upload(root: Path, client: FakeS3, now: datetime) -> None:
    nightly_upload(root, TARGET, client=client, clock=ManualClock(now), calendar=CALENDAR)


def test_a_switch_back_and_a_return_each_resume_the_nightly_upload(tmp_path, monkeypatch):
    # The laptop uploads, the VM is restored from the bucket and uploads its Tuesday session,
    # and the shadow laptop's sweep grows a tail.
    laptop, vm, client, files = _switched(tmp_path)
    vm_tuesday = client.body(MANIFEST_KEY)
    bucket_quarantine = client.body(TARGET.key(QUARANTINE))

    # The switch back. The laptop's nightly upload refuses, and so does the first upload.
    with pytest.raises(bucket.WatermarkMissing) as refused:
        _upload(laptop, client, TUESDAY_20)
    assert bucket.RESYNC_COMMAND in str(refused.value)
    with pytest.raises(bucket.FirstUploadRefused):
        first_upload(
            laptop, TARGET, client=client, clock=ManualClock(TUESDAY_20), calendar=CALENDAR
        )
    # The resuming host is still the shadow when it runs the resync.
    laptop_config = _config(tmp_path / "laptop-config", laptop, role="shadow")
    before = _snapshot(laptop)
    assert _main(laptop_config, client, monkeypatch) == 0
    assert _snapshot(laptop) == before
    assert _main(laptop_config, client, monkeypatch, "--apply") == 0
    assert manifest_path(laptop).read_bytes() == vm_tuesday
    # The VM's files came down, the laptop's own bars partition went, and the quarantine
    # ledger is the bucket's.
    for rel, data in files.items():
        assert (laptop / rel).read_bytes() == data
    assert not (laptop / BARS_2).exists()
    assert (laptop / QUARANTINE).read_bytes() == bucket_quarantine

    # The laptop's Wednesday session uploads, and the bucket still names the VM's session.
    _record(laptop, SPY_3, b"laptop chains 2026-08-26")
    _upload(laptop, client, WEDNESDAY_19)
    laptop_wednesday = client.body(MANIFEST_KEY)
    assert laptop_wednesday.startswith(vm_tuesday)
    assert f'"{SPY_2}"'.encode() in laptop_wednesday
    assert client.body(TARGET.key(SPY_2)) == files[SPY_2]

    # The shadow VM captured the same session with its own bytes, and a segment beside it.
    _record(vm, SEGMENT_3, b"vm segment", source="capture")
    _record(vm, SPY_3, b"vm chains 2026-08-26")

    # The return to the VM. Its upload refuses, and so does the first upload, and the resync
    # brings it level.
    with pytest.raises(bucket.WatermarkMissing) as refused:
        _upload(vm, client, WEDNESDAY_20)
    assert bucket.RESYNC_COMMAND in str(refused.value)
    with pytest.raises(bucket.FirstUploadRefused):
        first_upload(vm, TARGET, client=client, clock=ManualClock(WEDNESDAY_20), calendar=CALENDAR)
    vm_config = _config(tmp_path / "vm-config", vm, role="shadow")
    before = _snapshot(vm)
    assert _main(vm_config, client, monkeypatch, now=WEDNESDAY_20) == 0
    assert _snapshot(vm) == before
    assert _main(vm_config, client, monkeypatch, "--apply", now=WEDNESDAY_20) == 0
    assert manifest_path(vm).read_bytes() == laptop_wednesday
    assert (vm / SPY_3).read_bytes() == b"laptop chains 2026-08-26"
    assert not (vm / SEGMENT_3).exists()

    # The VM's Thursday session uploads with the laptop's Wednesday session still named.
    _record(vm, SPY_4, b"vm chains 2026-08-27")
    _upload(vm, client, THURSDAY_19)
    assert client.body(MANIFEST_KEY).startswith(laptop_wednesday)
    assert client.body(TARGET.key(SPY_3)) == b"laptop chains 2026-08-26"
