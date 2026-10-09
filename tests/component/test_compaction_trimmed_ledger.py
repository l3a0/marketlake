"""Compaction's repair of the trimmed ledger's manifest entry, before the upload reads it.

Marketlake #787. A trim or range-restore line that landed without its manifest entry refresh
leaves the entry's sha behind the ledger's bytes. Compaction re-records it through
``trimmed.repair_trimmed_entry`` after the re-tune and just before ``backup.sync``. A refusal
pages once and the backup still runs. What happens next depends on where the stale entry sits
against the bucket's watermark:

1. Behind it, the repair re-records the entry, the upload sends the ledger, and the ping goes.
2. Past it with a torn tail, the repair refuses and pages, the upload raises
   ``ChecksumRefused`` naming the trimmed ledger, and the ping is withheld.

A host with no ledger is byte-identical to one where the repair does not exist.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lake import compact as compact_module
from lake.alert import Publisher
from lake.bucket import BucketBackup, ChecksumRefused, first_upload
from lake.compact import (
    TRIMMED_LEDGER_EVENT,
    TRIMMED_LEDGER_TITLE,
    LedgerRepair,
    compact,
)
from lake.config import BucketTarget
from lake.lock import lake_lock
from lake.manifest import latest_entries, manifest_path, read_manifest, sha256_file
from lake.paths import TRIMMED_FILE, LakePaths
from lake.trimmed import append_trimmed, restore_line, trimmed_path
from tests.component.test_compaction import (
    DAY,
    FRIDAY,
    URL,
    _calendar,
    _chains,
    _clock_at,
    _segment,
    _snap,
)
from tests.support.backup import FakeBackup
from tests.support.bucket import FakeS3
from tests.support.lake import FixtureLake
from tests.support.pinger import FakePinger
from tests.support.transport import FakeTransport

TARGET = BucketTarget(bucket="lake-backup", prefix="lake")
FRIDAY_PARTITION = f"chains/ticker=SPY/date={FRIDAY.isoformat()}.parquet"


def _lake(lake_root: Path) -> None:
    FixtureLake(lake_root).with_partition(
        "chains", "SPY", FRIDAY, _chains(1, snap_ts=_snap(FRIDAY, 0))
    ).build()


def _line(lake_root: Path) -> dict:
    """A restore line for Friday's partition, the shape a range restore writes."""
    return restore_line(
        FRIDAY_PARTITION,
        sha256=sha256_file(lake_root / FRIDAY_PARTITION),
        restored_at="2026-08-21T19:00:00-04:00",
    )


def _append(lake_root: Path) -> None:
    with lake_lock(lake_root):
        append_trimmed(lake_root, _line(lake_root), source="range-restore", fetched_at=None)


def _seed(lake_root: Path) -> FakeS3:
    client = FakeS3()
    first_upload(
        lake_root, TARGET, client=client, clock=_clock_at(FRIDAY, 19, 0), calendar=_calendar()
    )
    client.calls.clear()
    return client


def _job(lake_root: Path, client: FakeS3, events: list[str], publisher: Publisher):
    clock = _clock_at(DAY, 16, 30)
    client.on_put = lambda kwargs, data: events.append(f"put {kwargs['Key']}")
    return compact(
        lake_root,
        clock=clock,
        calendar=_calendar(),
        backup=BucketBackup(client=client, clock=clock, calendar=_calendar()),
        backup_target=TARGET,
        pinger=FakePinger(events),
        ping_url=URL,
        publisher=publisher,
        plan_path=lake_root.parent / "chain_plan.json",
    )


def _paging(lake_root: Path) -> tuple[Publisher, FakeTransport]:
    transport = FakeTransport()
    return Publisher(lake_root=lake_root, transport=transport, secrets=()), transport


def _tonight(lake_root: Path) -> str:
    _segment(lake_root, "chains", "SPY", DAY, _chains(2, snap_ts=_snap(DAY, 0)), start_ts="a")
    return LakePaths(lake_root).chains_partition_path("SPY", DAY).relative_to(lake_root).as_posix()


def test_a_stale_entry_behind_the_watermark_is_re_recorded_after_the_seal(lake_root):
    """Mutations this catches: removing the repair, moving it to the start of the hold, and
    gating it on the window key, which this run never passes.
    """
    _lake(lake_root)
    _append(lake_root)
    client = _seed(lake_root)
    # A second line lands without its entry refresh, the state a crash inside the append
    # leaves. The entry it should have refreshed sits behind the bucket's watermark.
    with trimmed_path(lake_root).open("a") as ledger:
        ledger.write(json.dumps(_line(lake_root)) + "\n")
    tonight = _tonight(lake_root)
    events: list[str] = []
    publisher, transport = _paging(lake_root)

    result = _job(lake_root, client, events, publisher)

    assert result.ledger_repair == LedgerRepair(rerecorded=True)
    assert latest_entries(lake_root)[TRIMMED_FILE]["sha256"] == sha256_file(trimmed_path(lake_root))
    # The repair ran after tonight's seal, so its manifest line follows the seal's.
    order = [entry["partition"] for entry in read_manifest(lake_root)]
    assert order.index(tonight) < len(order) - 1 - order[::-1].index(TRIMMED_FILE)
    # The upload sent the ledger's current bytes, and the ping came after the manifest.
    assert client.body(TARGET.key(TRIMMED_FILE)) == trimmed_path(lake_root).read_bytes()
    assert events[-2:] == [f"put {TARGET.key('manifest.jsonl')}", "ping"]
    assert result.pinged
    assert transport.messages == []
    assert result.changed
    assert "  ledger   re-recorded the trimmed ledger's manifest entry" in result.render()


def test_the_repair_runs_after_the_re_tune_and_before_the_upload(lake_root, monkeypatch):
    """Mutation this catches: moving the repair to between the seal loop and the re-tune,
    which keeps its manifest line after the seal's and so passes the test above.
    """
    _lake(lake_root)
    _append(lake_root)
    client = _seed(lake_root)
    with trimmed_path(lake_root).open("a") as ledger:
        ledger.write(json.dumps(_line(lake_root)) + "\n")
    _tonight(lake_root)
    events: list[str] = []
    real_retune = compact_module._retune
    real_repair = compact_module._repair_trimmed_ledger

    def retune(*args, **kwargs):
        events.append("retune")
        return real_retune(*args, **kwargs)

    def repair(*args, **kwargs):
        events.append("repair")
        return real_repair(*args, **kwargs)

    monkeypatch.setattr(compact_module, "_retune", retune)
    monkeypatch.setattr(compact_module, "_repair_trimmed_ledger", repair)
    publisher, _ = _paging(lake_root)

    result = _job(lake_root, client, events, publisher)

    assert result.retune is not None
    assert result.ledger_repair == LedgerRepair(rerecorded=True)
    assert events[:2] == ["retune", "repair"]
    assert events[2].startswith("put ")
    assert events[-1] == "ping"


def test_a_torn_tail_past_the_watermark_pages_and_the_upload_withholds_the_ping(lake_root):
    _lake(lake_root)
    client = _seed(lake_root)
    # The ledger's first line and its entry land after the bucket's last upload, so the entry
    # is past the watermark, and then a write tears.
    _append(lake_root)
    with trimmed_path(lake_root).open("a") as ledger:
        ledger.write('{"kind": "tr')
    tonight = _tonight(lake_root)
    events: list[str] = []
    publisher, transport = _paging(lake_root)

    with pytest.raises(ChecksumRefused) as refused:
        _job(lake_root, client, events, publisher)

    assert "S3 refused trimmed.jsonl: " in str(refused.value)
    assert "A fourth cause is a trim or restore line" in str(refused.value)
    assert "ping" not in events
    # The seal stood, and the page went out before the upload.
    assert (lake_root / tonight).exists()
    assert len(transport.messages) == 1
    page = transport.messages[0]
    assert page.event == TRIMMED_LEDGER_EVENT
    assert page.title == TRIMMED_LEDGER_TITLE
    assert "Repair by hand under the lock" in page.body


def test_a_refused_repair_behind_the_watermark_still_backs_up_and_pings(lake_root):
    _lake(lake_root)
    _append(lake_root)
    client = _seed(lake_root)
    with trimmed_path(lake_root).open("a") as ledger:
        ledger.write('{"kind": "tr')
    _tonight(lake_root)
    events: list[str] = []
    publisher, transport = _paging(lake_root)

    result = _job(lake_root, client, events, publisher)

    assert result.backed_up and result.pinged
    assert result.ledger_repair is not None
    assert not result.ledger_repair.rerecorded
    assert result.ledger_repair.refusal is not None
    assert "no terminating newline" in result.ledger_repair.refusal
    assert f"  ledger   not repaired: {result.ledger_repair.refusal}" in result.render()
    assert [message.event for message in transport.messages] == [TRIMMED_LEDGER_EVENT]
    # The bucket's copy of the ledger lags rather than taking the torn bytes.
    assert TARGET.key(TRIMMED_FILE) not in client.put_keys()


def _quiet_run(lake_root: Path):
    """A run with nothing to seal, so the repair is the only thing that can change the lake."""
    return compact(
        lake_root,
        clock=_clock_at(DAY, 16, 30),
        calendar=_calendar(),
        backup=FakeBackup([]),
        backup_target=Path("/ssd/lake"),
        plan_path=lake_root.parent / "chain_plan.json",
    )


def test_a_re_recorded_entry_alone_is_a_change(lake_root):
    _lake(lake_root)
    _append(lake_root)
    with trimmed_path(lake_root).open("a") as ledger:
        ledger.write(json.dumps(_line(lake_root)) + "\n")
    result = _quiet_run(lake_root)
    assert result.sealed == () and result.verified == ()
    assert result.ledger_repair == LedgerRepair(rerecorded=True)
    assert result.changed
    # A second run finds the entry in step and changes nothing.
    again = _quiet_run(lake_root)
    assert again.ledger_repair is None
    assert not again.changed


def test_a_refused_repair_alone_is_not_a_change(lake_root):
    _lake(lake_root)
    _append(lake_root)
    with trimmed_path(lake_root).open("a") as ledger:
        ledger.write('{"kind": "tr')
    result = _quiet_run(lake_root)
    assert result.ledger_repair is not None and result.ledger_repair.refusal is not None
    assert not result.changed


def _snapshot(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_a_host_with_no_ledger_is_byte_identical(tmp_path, monkeypatch):
    def run(root: Path):
        _lake(root)
        _tonight(root)
        return compact(
            root,
            clock=_clock_at(DAY, 16, 30),
            calendar=_calendar(),
            backup=FakeBackup([]),
            backup_target=Path("/ssd/lake"),
            plan_path=root.parent / "plan.json",
        )

    with_repair = run(tmp_path / "a" / "lake")
    monkeypatch.setattr(compact_module, "_repair_trimmed_ledger", lambda *a, **k: None)
    without = run(tmp_path / "b" / "lake")

    assert with_repair.ledger_repair is None
    assert _snapshot(tmp_path / "a" / "lake") == _snapshot(tmp_path / "b" / "lake")
    assert not trimmed_path(tmp_path / "a" / "lake").exists()
    assert with_repair.render() == without.render()
    assert TRIMMED_FILE not in manifest_path(tmp_path / "a" / "lake").read_text()
