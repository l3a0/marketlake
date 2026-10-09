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


def _resync(
    root: Path,
    client: FakeS3,
    *,
    now: datetime = TUESDAY_20,
    free: int = PLENTY,
    euid: int = OWNER,
) -> bucket.ResyncSummary:
    return resync(
        root,
        TARGET,
        client=client,
        clock=ManualClock(now),
        calendar=CALENDAR,
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


def test_a_file_only_this_lake_holds_with_other_bytes_on_disk_is_not_refused(tmp_path):
    laptop, vm, client, _files = _switched(tmp_path)
    rel = "chains/ticker=QQQ/date=2026-08-25.parquet"
    _record(laptop, rel, b"recorded", source="capture")
    (laptop / rel).write_bytes(b"replaced since")

    assert _resync(laptop, client).downloads


def test_a_capture_spans_rewrite_in_this_lakes_tail_refuses(tmp_path):
    laptop, vm, client, _files = _switched(tmp_path)
    _record(laptop, SPANS, b"the laptop's spans", source="reference")

    message = _refused(laptop, client)

    assert "human decision" in message
    assert repr(SPANS) in message
    (laptop / SPANS).write_bytes(b"moved aside and replaced")
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
        f"resync: dry run: 3 download(s), 0.0 MB, 1 deletion(s), must stop by {WEDNESDAY_BOUND}"
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
