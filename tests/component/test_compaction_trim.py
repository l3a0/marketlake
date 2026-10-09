"""The trim's slot inside the close+15 compaction, its gates, and the empty-directory pass.

Marketlake #787. ``compact`` judges the window key and the role, requires a bucket backup
whose upload left a deadline and a watermark, and runs the trim after the ping, still in the
lock hold. A trim failure withholds no ping and never raises out of ``compact``. After the trim
step, on every exit from it, a pass removes each empty ``chains/ticker=T/`` on any host that
holds a ``trimmed.jsonl``, a shadow and a host with no window included.

A window cannot go below ``window.window_floor`` under the default guards, and the window
of 22 these tests use clears it. So they use a calendar of six weeks, from 2026-07-20 to
2026-08-28. Tonight is Friday 2026-08-28. With a window of 22 the edge is 2026-07-29, so the
partitions of 2026-07-20 and 2026-07-21 are past it.
"""

from __future__ import annotations

import errno
import hashlib
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from lake import bucket
from lake import compact as compact_module
from lake.alert import Publisher
from lake.bucket import BucketBackup, first_upload, nightly_upload, restore_lake
from lake.calendar import MARKET_TZ
from lake.compact import TRIM_ROT_EVENT, TRIM_ROT_TITLE, compact
from lake.config import BucketTarget
from lake.lock import lake_lock
from lake.manifest import latest_entries
from lake.trimmed import append_trimmed, restore_line, trimmed_path
from tests.component.test_compaction import _chains, _segment, _snap
from tests.component.test_trim import _checkpoint
from tests.support.backup import FakeBackup
from tests.support.bucket import FakeS3, unreachable
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.lake import FixtureLake
from tests.support.pinger import FakePinger
from tests.support.transport import FakeTransport

TARGET = BucketTarget(bucket="lake-backup", prefix="lake")
MONDAYS = [date(2026, 7, 20) + timedelta(weeks=week) for week in range(6)]
CALENDAR = weekday_sessions(*MONDAYS)
TONIGHT = date(2026, 8, 28)
YESTERDAY = date(2026, 8, 27)
OLD = (date(2026, 7, 20), date(2026, 7, 21))
EDGE = date(2026, 7, 29)
WINDOW = 22
URL = "https://hc-ping.com/secret-key/compaction"


def _et(day: date, hour: int, minute: int) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=MARKET_TZ)


def _rel(ticker: str, day: date) -> str:
    return f"chains/ticker={ticker}/date={day.isoformat()}.parquet"


def _lake(
    root: Path,
    *,
    tickers=("SPY",),
    seed: bool = True,
    ledger: bool = False,
    tonight: bool = True,
) -> FakeS3:
    """Old partitions past the edge, one inside it, a checkpoint, and a seeded bucket."""
    lake = FixtureLake(root)
    for ticker in tickers:
        for day in (*OLD, YESTERDAY):
            lake.with_partition(
                "chains", ticker, day, _chains(1, snap_ts=_snap(day, 0), ticker=ticker)
            )
    lake.build()
    _checkpoint(root, YESTERDAY, dict.fromkeys(tickers, EDGE))
    if ledger:
        _ledger(root)
    client = FakeS3()
    if seed:
        first_upload(
            root, TARGET, client=client, clock=ManualClock(_et(YESTERDAY, 19, 0)), calendar=CALENDAR
        )
        client.calls.clear()
    # Tonight's capture, which this run seals.
    if tonight:
        _segment(
            root, "chains", "SPY", TONIGHT, _chains(2, snap_ts=_snap(TONIGHT, 0)), start_ts="a"
        )
    return client


def _job(
    root: Path,
    client: FakeS3,
    events: list[str],
    *,
    role: str | None = "primary",
    window: int | str | None = WINDOW,
    clock: ManualClock | None = None,
    publisher: Publisher | None = None,
    backup=None,
    backup_target=TARGET,
):
    clock = clock if clock is not None else ManualClock(_et(TONIGHT, 16, 30))
    client.on_put = lambda kwargs, data: events.append(f"put {TARGET.rel(kwargs['Key'])}")
    previous = client.on_get

    def on_get(kwargs):
        events.append(f"get {TARGET.rel(kwargs['Key'])}")
        if previous is not None:
            previous(kwargs)

    client.on_get = on_get
    return compact(
        root,
        clock=clock,
        calendar=CALENDAR,
        backup=(
            backup
            if backup is not None
            else BucketBackup(client=client, clock=clock, calendar=CALENDAR)
        ),
        backup_target=backup_target,
        pinger=FakePinger(events),
        ping_url=URL,
        publisher=publisher,
        plan_path=root.parent / "chain_plan.json",
        role=role,
        window_sessions=window,
    )


# -- the slot ---------------------------------------------------------------------------


def test_the_trim_runs_after_the_ping_and_drops_what_the_window_allows(lake_root):
    """Mutation this catches: trimming before ``backup.sync`` or before the ping."""
    client = _lake(lake_root)
    events: list[str] = []

    result = _job(lake_root, client, events)

    assert result.trim is not None
    assert list(result.trim.trimmed) == [_rel("SPY", day) for day in OLD]
    assert result.trim.edge == EDGE
    ping = events.index("ping")
    assert events.index(f"put {'manifest.jsonl'}") < ping
    assert [event for event in events[ping + 1 :]] == [f"get {_rel('SPY', day)}" for day in OLD]
    assert result.pinged and result.backed_up
    assert result.changed
    rendered = result.render().splitlines()
    assert rendered[-1].startswith("  trim     window=22 edge=2026-07-29 trimmed=2 ")
    assert (lake_root / _rel("SPY", YESTERDAY)).exists()


def test_a_run_whose_only_change_is_the_trim_reports_a_change(lake_root):
    client = _lake(lake_root, tonight=False)
    result = _job(lake_root, client, [])
    assert result.sealed == () and result.ledger_repair is None and result.pruned == ()
    assert result.trim is not None and result.trim.trimmed
    assert result.changed
    again = _job(lake_root, client, [])
    assert again.trim is not None and again.trim.trimmed == ()
    assert not again.changed


def test_an_unforeseen_error_in_the_trim_step_leaves_the_ping_sent(lake_root, monkeypatch):
    from lake import trim as trim_module

    client = _lake(lake_root)

    def broken(*args, **kwargs):
        raise RuntimeError("a bug")

    monkeypatch.setattr(trim_module, "trim", broken)
    result = _job(lake_root, client, [])
    assert result.pinged and result.sealed
    assert result.trim is not None and "RuntimeError" in str(result.trim.stopped)


def test_a_trim_that_meets_an_unreachable_bucket_leaves_the_ping_sent(lake_root):
    """Mutation this catches: letting a trim error propagate."""
    client = _lake(lake_root)

    def down(kwargs):
        raise unreachable()

    client.on_get = down
    events: list[str] = []
    result = _job(lake_root, client, events)
    assert result.pinged
    assert result.trim is not None and result.trim.stopped is not None
    assert result.sealed and result.trim.trimmed == ()


def test_the_trim_stops_at_the_uploads_deadline(lake_root):
    client = _lake(lake_root)
    clock = ManualClock(_et(TONIGHT, 16, 30))
    # The first read takes the whole 75-minute budget, so the second partition waits.
    client.on_get = lambda kwargs: clock.advance(bucket.NIGHTLY_UPLOAD_BUDGET.total_seconds())
    result = _job(lake_root, client, [], clock=clock)
    assert result.trim is not None
    assert list(result.trim.trimmed) == [_rel("SPY", OLD[0])]
    assert result.trim.deadline is not None
    assert (lake_root / _rel("SPY", OLD[1])).exists()


def test_main_prints_the_nights_sealed_lines_when_the_trim_stops(
    lake_root, tmp_path, monkeypatch, capsys
):
    client = _lake(lake_root)

    def down(kwargs):
        raise unreachable()

    client.on_get = down
    config = write_config(tmp_path, lake_root, role="primary")
    text = config.read_text().replace(
        f"backup_target: {tmp_path / 'ssd'}", f"backup_target: {TARGET}"
    )
    config.write_text(
        text
        + "bucket_access_key_id: AKIDCONFIG\n"
        + "bucket_secret_access_key: secret-bucket-key\n"
        + "bucket_region: us-east-2\n"
        + f"lake_window_sessions: {WINDOW}\n"
    )
    pinger = FakePinger()
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    monkeypatch.setattr("lake.runner.UrllibPinger", lambda: pinger)

    code = compact_module.main(
        ["--config", str(config), "--plan", str(tmp_path / "chain_plan.json")],
        clock=ManualClock(_et(TONIGHT, 16, 30)),
        calendar=CALENDAR,
    )

    assert code == 0
    out = capsys.readouterr().out
    assert f"  sealed   chains/ticker=SPY/date={TONIGHT.isoformat()}.parquet" in out
    assert "  trim     stopped: the bucket unreachable the read of " in out
    assert len(pinger.urls) == 1


def test_main_passes_the_resolved_role_and_the_key(lake_root, tmp_path, monkeypatch, capsys):
    # A config with no role key reads as primary, and the trim runs under it.
    client = _lake(lake_root)
    config = write_config(tmp_path, lake_root)
    text = config.read_text().replace(
        f"backup_target: {tmp_path / 'ssd'}", f"backup_target: {TARGET}"
    )
    assert "role:" not in text
    config.write_text(
        text
        + "bucket_access_key_id: AKIDCONFIG\n"
        + "bucket_secret_access_key: secret-bucket-key\n"
        + "bucket_region: us-east-2\n"
        + f"lake_window_sessions: {WINDOW}\n"
    )
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    monkeypatch.setattr("lake.runner.UrllibPinger", lambda: FakePinger())
    assert (
        compact_module.main(
            ["--config", str(config), "--plan", str(tmp_path / "chain_plan.json")],
            clock=ManualClock(_et(TONIGHT, 16, 30)),
            calendar=CALENDAR,
        )
        == 0
    )
    assert "  trim     window=22 edge=2026-07-29 trimmed=2 " in capsys.readouterr().out
    assert not (lake_root / _rel("SPY", OLD[0])).exists()


def test_two_rotted_copies_send_one_page(lake_root):
    """Mutation this catches: a page per partition."""
    client = _lake(lake_root, tickers=("QQQ", "SPY"))
    for ticker in ("QQQ", "SPY"):
        client.store(TARGET.key(_rel(ticker, OLD[0])), b"rotted")
    transport = FakeTransport()
    publisher = Publisher(lake_root=lake_root, transport=transport, secrets=())
    events: list[str] = []
    result = _job(lake_root, client, events, publisher=publisher)
    assert result.trim is not None and len(result.trim.rot) == 2
    (page,) = transport.messages
    assert page.event == TRIM_ROT_EVENT and page.title == TRIM_ROT_TITLE
    assert _rel("QQQ", OLD[0]) in page.body and _rel("SPY", OLD[0]) in page.body
    # Nothing was put to the bucket after the ping, so the trim wrote nothing there.
    after = events[events.index("ping") + 1 :]
    assert after and not any(event.startswith("put ") for event in after)
    assert all((lake_root / _rel(ticker, OLD[0])).exists() for ticker in ("QQQ", "SPY"))


# -- the refusals --------------------------------------------------------------------------


def test_a_host_with_no_window_never_trims_and_prints_nothing_new(lake_root):
    client = _lake(lake_root)
    result = _job(lake_root, client, events := [], window=None)
    assert result.trim is None
    assert not any(event.startswith("get ") for event in events)
    assert "trim" not in result.render()


@pytest.mark.parametrize(
    ("kwargs", "words"),
    [
        ({"window": 5}, "under the floor"),
        ({"window": "'22'"}, "not a whole number"),
        ({"role": "shadow"}, "the role is 'shadow'"),
        ({"role": None}, "the role is None"),
    ],
    ids=["under-the-floor", "not-a-number", "shadow", "no-role"],
)
def test_a_gate_that_refuses_keeps_every_partition(lake_root, kwargs, words):
    client = _lake(lake_root)
    events: list[str] = []
    result = _job(lake_root, client, events, **kwargs)
    assert result.trim is not None and result.trim.refused is not None
    assert words in result.trim.refused
    assert not any(event.startswith("get ") for event in events)
    assert all((lake_root / _rel("SPY", day)).exists() for day in OLD)
    assert result.pinged
    assert f"  trim     refused: {result.trim.refused}" in result.render()


def test_a_runner_that_is_not_a_bucket_refuses_even_with_a_bucket_target(lake_root):
    """Mutation this catches: judging the target's form alone. A runner that is not a bucket
    backup carries no upload summary to trim by."""
    client = _lake(lake_root)
    events: list[str] = []
    result = _job(lake_root, client, events, backup=FakeBackup([]), backup_target=TARGET)
    assert result.trim is not None and result.trim.stopped is None
    assert result.trim.refused is not None and "not a bucket" in result.trim.refused
    assert not any(event.startswith("get ") for event in events)
    assert all((lake_root / _rel("SPY", day)).exists() for day in OLD)


def test_main_on_a_shadow_refuses_the_trim_by_its_role(lake_root, tmp_path, monkeypatch, capsys):
    """Mutation this catches: ``main`` passing a fixed role. A shadow builds no backup, so a
    fixed primary role would still refuse, but by the backup, not by the role."""
    client = _lake(lake_root)
    config = write_config(tmp_path, lake_root, role="shadow")
    text = config.read_text().replace(
        f"backup_target: {tmp_path / 'ssd'}", f"backup_target: {TARGET}"
    )
    config.write_text(
        text
        + "bucket_access_key_id: AKIDCONFIG\n"
        + "bucket_secret_access_key: secret-bucket-key\n"
        + "bucket_region: us-east-2\n"
        + f"lake_window_sessions: {WINDOW}\n"
    )
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    monkeypatch.setattr("lake.runner.UrllibPinger", lambda: FakePinger())
    compact_module.main(
        ["--config", str(config), "--plan", str(tmp_path / "chain_plan.json")],
        clock=ManualClock(_et(TONIGHT, 16, 30)),
        calendar=CALENDAR,
    )
    out = capsys.readouterr().out
    assert "  trim     refused: the role is 'shadow'" in out, out
    assert all((lake_root / _rel("SPY", day)).exists() for day in OLD)


def test_a_path_backup_target_refuses_even_with_a_runner(lake_root):
    """Mutation this catches: gating on ``backup is not None`` alone."""
    client = _lake(lake_root)
    result = _job(
        lake_root, client, [], backup=FakeBackup([]), backup_target=lake_root.parent / "ssd"
    )
    assert result.trim is not None
    assert result.trim.refused is not None and "not a bucket" in result.trim.refused
    assert all((lake_root / _rel("SPY", day)).exists() for day in OLD)


def test_a_shadow_with_no_backup_refuses(lake_root):
    client = _lake(lake_root)
    events: list[str] = []
    result = compact(
        lake_root,
        clock=ManualClock(_et(TONIGHT, 16, 30)),
        calendar=CALENDAR,
        backup=None,
        backup_target=TARGET,
        plan_path=lake_root.parent / "chain_plan.json",
        role="shadow",
        window_sessions=WINDOW,
    )
    assert result.trim is not None and "the role is 'shadow'" in str(result.trim.refused)
    assert client.calls == [] and events == []


def test_a_refused_ledger_repair_skips_the_trim(lake_root):
    # The ledger's entry sits behind the watermark, so the upload carries on past the refusal
    # and the trim is what the refusal stops.
    client = _lake(lake_root, ledger=True)
    with trimmed_path(lake_root).open("a") as ledger:
        ledger.write('{"kind": "tr')
    events: list[str] = []
    result = _job(lake_root, client, events)
    assert result.pinged
    assert result.ledger_repair is not None and result.ledger_repair.refusal is not None
    assert result.trim is not None and "was not repaired" in str(result.trim.refused)
    assert not any(event.startswith("get ") for event in events)
    assert all((lake_root / _rel("SPY", day)).exists() for day in OLD)


# -- the empty-directory pass ----------------------------------------------------------------


def test_a_ticker_whose_last_partition_is_trimmed_loses_its_directory(lake_root):
    client = _lake(lake_root, tickers=("QQQ", "SPY"))
    # QQQ's partition inside the window goes, so its last ones are the trimmed ones.
    (lake_root / _rel("QQQ", YESTERDAY)).unlink()
    result = _job(lake_root, client, [])
    assert result.trim is not None
    assert _rel("QQQ", OLD[0]) in result.trim.trimmed
    assert not (lake_root / "chains" / "ticker=QQQ").exists()
    assert (lake_root / "chains" / "ticker=SPY").is_dir()
    assert result.pruned == ("chains/ticker=QQQ",)
    assert "  pruned   chains/ticker=QQQ/ held no partition" in result.render()


def _empty_ticker(root: Path) -> Path:
    path = root / "chains" / "ticker=IWM"
    path.mkdir(parents=True)
    return path


def _ledger(root: Path) -> None:
    with lake_lock(root):
        append_trimmed(
            root,
            # A restore line for a ticker this lake does not hold, so it keeps nothing here.
            restore_line(_rel("IWM", OLD[0]), sha256="x", restored_at="2026-08-27T19:00:00-04:00"),
            source="range-restore",
            fetched_at=None,
        )


def test_a_directory_a_crash_left_empty_goes_on_a_run_stopped_at_the_deadline(lake_root):
    """Mutation this catches: running the pass only on a normal end."""
    client = _lake(lake_root)
    _ledger(lake_root)
    left = _empty_ticker(lake_root)
    clock = ManualClock(_et(TONIGHT, 16, 30))
    client.on_get = lambda kwargs: clock.advance(bucket.NIGHTLY_UPLOAD_BUDGET.total_seconds())
    result = _job(lake_root, client, [], clock=clock)
    assert result.trim is not None and result.trim.deadline is not None
    assert not left.exists()


@pytest.mark.parametrize("stop", ["refused-gate", "stopped"])
def test_the_pass_runs_on_a_refused_gate_and_a_stopped_trim(lake_root, stop):
    client = _lake(lake_root)
    _ledger(lake_root)
    left = _empty_ticker(lake_root)
    if stop == "stopped":
        client.on_get = lambda kwargs: (_ for _ in ()).throw(unreachable())
        result = _job(lake_root, client, [])
        assert result.trim is not None and result.trim.stopped is not None
    else:
        result = _job(lake_root, client, [], window=5)
        assert result.trim is not None and result.trim.refused is not None
    assert not left.exists()


def test_a_host_with_a_ledger_and_no_window_loses_an_empty_directory(lake_root):
    """Mutation this catches: gating the pass on the window key."""
    _lake(lake_root, seed=False)
    _ledger(lake_root)
    left = _empty_ticker(lake_root)
    result = compact(
        lake_root,
        clock=ManualClock(_et(TONIGHT, 16, 30)),
        calendar=CALENDAR,
        backup=FakeBackup([]),
        backup_target=lake_root.parent / "ssd",
        plan_path=lake_root.parent / "chain_plan.json",
    )
    assert result.trim is None
    assert not left.exists()
    assert result.pruned == ("chains/ticker=IWM",)


def test_a_shadow_with_a_ledger_loses_an_empty_directory(lake_root):
    _lake(lake_root, seed=False)
    _ledger(lake_root)
    left = _empty_ticker(lake_root)
    compact(
        lake_root,
        clock=ManualClock(_et(TONIGHT, 16, 30)),
        calendar=CALENDAR,
        backup=None,
        backup_target=TARGET,
        plan_path=lake_root.parent / "chain_plan.json",
        role="shadow",
    )
    assert not left.exists()


def test_a_host_with_no_ledger_keeps_an_empty_directory(lake_root):
    _lake(lake_root, seed=False)
    left = _empty_ticker(lake_root)
    result = compact(
        lake_root,
        clock=ManualClock(_et(TONIGHT, 16, 30)),
        calendar=CALENDAR,
        backup=FakeBackup([]),
        backup_target=lake_root.parent / "ssd",
        plan_path=lake_root.parent / "chain_plan.json",
    )
    assert left.is_dir()
    assert result.pruned == ()
    assert not trimmed_path(lake_root).exists()


def test_a_run_whose_only_change_is_a_pruned_directory_reports_a_change(lake_root):
    """Mutation this catches: ``changed`` ignoring the pass. The first run seals and writes
    the plan, so only the second run's removal is left to count."""
    _lake(lake_root, seed=False, tonight=False)
    _ledger(lake_root)

    def run():
        return compact(
            lake_root,
            clock=ManualClock(_et(TONIGHT, 16, 30)),
            calendar=CALENDAR,
            backup=FakeBackup([]),
            backup_target=lake_root.parent / "ssd",
            plan_path=lake_root.parent / "chain_plan.json",
        )

    run()
    assert not run().changed
    left = _empty_ticker(lake_root)
    result = run()
    assert result.pruned == ("chains/ticker=IWM",) and not left.exists()
    assert result.trim is None and result.sealed == ()
    assert result.changed


def test_the_pass_keeps_an_empty_directory_that_is_not_a_ticker(lake_root):
    """Mutation this catches: removing every empty directory under ``chains/``. Only a
    ``ticker=`` directory is the trim's to remove."""
    _lake(lake_root, seed=False)
    _ledger(lake_root)
    other = lake_root / "chains" / "notaticker"
    other.mkdir()
    left = _empty_ticker(lake_root)
    result = compact(
        lake_root,
        clock=ManualClock(_et(TONIGHT, 16, 30)),
        calendar=CALENDAR,
        backup=FakeBackup([]),
        backup_target=lake_root.parent / "ssd",
        plan_path=lake_root.parent / "chain_plan.json",
    )
    assert other.is_dir()
    assert not left.exists()
    assert result.pruned == ("chains/ticker=IWM",)


def test_the_pass_keeps_a_directory_holding_anything(lake_root):
    _lake(lake_root, seed=False)
    _ledger(lake_root)
    held = _empty_ticker(lake_root)
    (held / "stray.txt").write_text("not a partition")
    compact(
        lake_root,
        clock=ManualClock(_et(TONIGHT, 16, 30)),
        calendar=CALENDAR,
        backup=FakeBackup([]),
        backup_target=lake_root.parent / "ssd",
        plan_path=lake_root.parent / "chain_plan.json",
    )
    assert (held / "stray.txt").exists()


def test_an_unlink_refusal_on_one_ticker_leaves_the_other_trimmed(lake_root, monkeypatch):
    client = _lake(lake_root, tickers=("QQQ", "SPY"))
    real = Path.unlink
    target = _rel("QQQ", OLD[0])

    def unlink(self, missing_ok=False):
        if self.as_posix().endswith(target):
            raise PermissionError(errno.EACCES, "Permission denied", str(self))
        return real(self, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", unlink)
    result = _job(lake_root, client, [])
    assert result.trim is not None
    assert list(result.trim.trimmed) == [_rel("SPY", day) for day in OLD]
    assert result.trim.held and result.trim.held[0].startswith("chains/ticker=QQQ/")
    assert (lake_root / target).exists()


# -- the trim feeds the rebuild ---------------------------------------------------------


def _chains_files(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted((root / "chains").rglob("*.parquet"))
    }


def test_the_real_trim_feeds_the_rebuild_and_a_whole_restore_brings_it_back(lake_root, tmp_path):
    """The trim's own ledger lines, uploaded the next night, drive marketlake #785's rebuild.

    The rebuild tests in ``tests/component/test_bucket_rebuild.py`` trim with a stand-in, so
    this is the one run of the real trim into the real restore. Mutation this catches: a trim
    line the rebuild's exclusion does not read as a designed absence, such as one recording the
    wrong sha or the wrong partition, which would bring every trimmed partition back.
    """
    tickers = ("QQQ", "SPY")
    client = _lake(lake_root, tickers=tickers)
    result = _job(lake_root, client, [])
    assert result.trim is not None
    gone = {_rel(ticker, day) for ticker in tickers for day in OLD}
    assert set(result.trim.trimmed) == gone
    with lake_lock(lake_root):
        nightly_upload(
            lake_root,
            TARGET,
            client=client,
            clock=ManualClock(_et(TONIGHT, 19, 0)),
            calendar=CALENDAR,
        )
    kept = _chains_files(lake_root)
    assert kept and not gone & set(kept)

    windowed = restore_lake(
        tmp_path / "windowed", TARGET, client=client, skip_designed_absences=True
    )
    whole = restore_lake(tmp_path / "whole", TARGET, client=client)

    assert (windowed.restored, windowed.failures, windowed.trimmed_lost) == (True, [], [])
    assert windowed.trimmed_left_out == len(gone)
    assert _chains_files(tmp_path / "windowed") == kept
    assert (whole.restored, whole.failures) == (True, [])
    restored = _chains_files(tmp_path / "whole")
    assert set(restored) == set(kept) | gone
    entries = latest_entries(lake_root)
    for rel in gone:
        assert hashlib.sha256(restored[rel]).hexdigest() == entries[rel]["sha256"]
