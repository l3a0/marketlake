"""The trim's slot inside the close+15 compaction, its gates, and the empty-directory pass.

Marketlake #787. ``compact`` judges the window key and the role, requires a bucket backup whose
upload left a deadline and a watermark, and runs the trim after the ping, still in the lock hold. A
trim failure withholds no ping and never raises out of ``compact``. After the trim step, on every
exit from it, a pass removes each empty ``chains/ticker=T/`` and ``quotes/ticker=T/`` on any host
that holds a ``trimmed.jsonl``, a shadow and a host with no window included.

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

import pyarrow as pa
import pytest

from lake import bucket, report
from lake import compact as compact_module
from lake.alert import Publisher
from lake.bucket import BucketBackup, first_upload, nightly_upload, restore_lake
from lake.calendar import MARKET_TZ
from lake.compact import EVENING_UPLOAD_SLUG, TRIM_ROT_EVENT, TRIM_ROT_TITLE, compact
from lake.config import BucketTarget
from lake.lock import lake_lock
from lake.manifest import latest_entries
from lake.trimmed import append_trimmed, restore_line, trimmed_path
from tests.component.test_compaction import _chains, _quotes, _segment, _snap
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


def _qrel(ticker: str, day: date) -> str:
    return f"quotes/ticker={ticker}/date={day.isoformat()}.parquet"


def _lake(
    root: Path,
    *,
    tickers=("SPY",),
    seed: bool = True,
    ledger: bool = False,
    tonight: bool = True,
    quotes: bool = False,
) -> FakeS3:
    """Old partitions past the edge, one inside it, a checkpoint, and a seeded bucket.

    ``quotes`` adds a quotes partition and a daily bar beside each chains partition.
    """
    lake = FixtureLake(root)
    for ticker in tickers:
        for day in (*OLD, YESTERDAY):
            lake.with_partition(
                "chains", ticker, day, _chains(1, snap_ts=_snap(day, 0), ticker=ticker)
            )
            if quotes:
                lake.with_partition(
                    "quotes", ticker, day, _quotes(1, snap_ts=_snap(day, 0), ticker=ticker)
                )
                lake.with_bars(ticker, "1d", day, pa.table({"close": [650.0]}))
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
    surfaces: dict[str, object] | None = None,
):
    """One compaction run. ``surfaces`` carries the per-surface keywords, such as
    ``window_sessions_bars``, as stored values."""
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
        **(surfaces or {}),
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


def _evening_config(tmp_path: Path, lake_root: Path) -> Path:
    """A primary's config with a bucket target and the window key, as the VM's carries."""
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
    return config


def _after_vendor_sweep(tmp_path: Path, config: Path) -> int:
    """The run the vendor sweep hands off to, at 18:32 on tonight's session (marketlake #833)."""
    return compact_module.main(
        [
            "--after-vendor-sweep",
            "--config",
            str(config),
            "--plan",
            str(tmp_path / "chain_plan.json"),
        ],
        clock=ManualClock(_et(TONIGHT, 18, 32)),
        calendar=CALENDAR,
    )


@pytest.mark.parametrize(
    ("checkpoint_day", "trimmed"),
    [(TONIGHT, False), (YESTERDAY, True)],
    ids=["tonights-checkpoint-refuses", "yesterdays-checkpoint-trims"],
)
def test_the_run_after_the_vendor_sweep_trims_nothing_by_tonights_checkpoint(
    lake_root, tmp_path, monkeypatch, capsys, checkpoint_day, trimmed
):
    """The 18:30 vendor sweep writes tonight's checkpoint, so its upload drops nothing.

    ``trim`` trims only on a session strictly after the checkpoint's own, and the vendor sweep
    writes ``Checkpoint(session_day=tonight)``. So the run it hands off to reaches the trim and
    is refused there, before any bucket read. The config carries the window key, a bucket
    target and a primary role, because without any one of them the trim is never reached and
    a case asserting only "no GET" would pass while proving nothing.

    The control case keeps yesterday's checkpoint, as on a night the vendor sweep wrote none,
    and the same run trims both old partitions. That is what shows the refusal line can fail.
    """
    client = _lake(lake_root, tonight=False)
    if checkpoint_day == TONIGHT:
        _checkpoint(lake_root, TONIGHT, {"SPY": EDGE})
    events: list[str] = []
    client.on_get = lambda kwargs: events.append(f"get {TARGET.rel(kwargs['Key'])}")
    pinger = FakePinger()
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    monkeypatch.setattr("lake.runner.UrllibPinger", lambda: pinger)

    code = _after_vendor_sweep(tmp_path, _evening_config(tmp_path, lake_root))

    assert code == 0
    out = capsys.readouterr().out
    assert pinger.urls == [f"https://hc-ping.com/secret-key/{EVENING_UPLOAD_SLUG}"]
    if trimmed:
        assert "  trim     window=22 edge=2026-07-29 trimmed=2 " in out, out
        assert events == [f"get {_rel('SPY', day)}" for day in OLD]
        assert not any((lake_root / _rel("SPY", day)).exists() for day in OLD)
    else:
        assert f"  trim     refused: the checkpoint is tonight's, {TONIGHT.isoformat()}" in out, out
        assert events == []
        assert not any(call[0] == "get_object" for call in client.calls), client.calls
        assert all((lake_root / _rel("SPY", day)).exists() for day in OLD)


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


def test_a_ticker_whose_last_quotes_day_is_trimmed_loses_its_quotes_directory(lake_root):
    """Marketlake #794. Mutation this catches: the prune over ``chains/`` alone. A retired
    ticker leaves the dashboard once its last quotes day goes, because ``lake_roster`` then
    finds no directory for it on either surface."""
    client = _lake(lake_root, tickers=("QQQ", "SPY"), quotes=True)
    (lake_root / _rel("QQQ", YESTERDAY)).unlink()
    (lake_root / _qrel("QQQ", YESTERDAY)).unlink()
    result = _job(lake_root, client, [])
    assert result.trim is not None
    assert {_qrel("QQQ", day) for day in OLD} <= set(result.trim.trimmed)
    assert {_qrel("SPY", day) for day in OLD} <= set(result.trim.trimmed)
    assert result.pruned == ("chains/ticker=QQQ", "quotes/ticker=QQQ")
    assert not (lake_root / "quotes" / "ticker=QQQ").exists()
    assert (lake_root / "quotes" / "ticker=SPY").is_dir()
    assert "  pruned   quotes/ticker=QQQ/ held no partition" in result.render()


def test_a_quotes_directory_left_empty_goes_on_a_host_with_a_ledger(lake_root):
    _lake(lake_root, seed=False)
    _ledger(lake_root)
    left = lake_root / "quotes" / "ticker=IWM"
    left.mkdir(parents=True)
    result = compact(
        lake_root,
        clock=ManualClock(_et(TONIGHT, 16, 30)),
        calendar=CALENDAR,
        backup=FakeBackup([]),
        backup_target=lake_root.parent / "ssd",
        plan_path=lake_root.parent / "chain_plan.json",
    )
    assert not left.exists()
    assert result.pruned == ("quotes/ticker=IWM",)


def test_the_pass_reaches_quotes_on_a_lake_with_no_chains_directory(lake_root):
    """Marketlake #794. A lake with no ``chains/`` cannot be listed there, and that surface's
    error must not end the scan. Mutation this catches: the listing's ``continue`` turned into
    ``break``, which never reaches ``quotes/``."""
    _ledger(lake_root)
    left = lake_root / "quotes" / "ticker=IWM"
    left.mkdir(parents=True)
    assert not (lake_root / "chains").exists()

    assert compact_module._prune_trimmed_tickers(lake_root) == ("quotes/ticker=IWM",)
    assert not left.exists()


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


def _chains_files(root: Path, surface: str = "chains") -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted((root / surface).rglob("*.parquet"))
    }


def test_the_real_trim_feeds_the_rebuild_and_a_whole_restore_brings_it_back(lake_root, tmp_path):
    """The trim's own ledger lines, uploaded the next night, drive marketlake #785's rebuild.

    The rebuild tests in ``tests/component/test_bucket_rebuild.py`` trim with a stand-in, so
    this is the one run of the real trim into the real restore. Mutation this catches: a trim
    line the rebuild's exclusion does not read as a designed absence, such as one recording the
    wrong sha or the wrong partition, which would bring every trimmed partition back.

    It also dates the trim's real ledger lines for marketlake #796. Close-guard reports filed on
    the newest trimmed day, the day after it and the trim's edge go up with the next night's
    upload, and only the first is left out. The edge is eight days past the newest trimmed day,
    so a cutoff taken from the edge would leave out all three.
    """
    tickers = ("QQQ", "SPY")
    client = _lake(lake_root, tickers=tickers)
    result = _job(lake_root, client, [])
    assert result.trim is not None
    gone = {_rel(ticker, day) for ticker in tickers for day in OLD}
    assert set(result.trim.trimmed) == gone
    guards = {}
    for day in (OLD[-1], OLD[-1] + timedelta(days=1), EDGE):
        path = report.close_guard_dir(lake_root, day) / "162000000000-4242.json"
        path.parent.mkdir(parents=True)
        path.write_text('{"report": true}\n')
        guards[day] = path.relative_to(lake_root).as_posix()
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
    assert windowed.reports_left_out == 1
    assert not (tmp_path / "windowed" / guards[OLD[-1]]).exists()
    assert (tmp_path / "windowed" / guards[OLD[-1] + timedelta(days=1)]).is_file()
    assert (tmp_path / "windowed" / guards[EDGE]).is_file()
    assert all((tmp_path / "whole" / rel).is_file() for rel in guards.values())
    assert (whole.restored, whole.failures) == (True, [])
    restored = _chains_files(tmp_path / "whole")
    assert set(restored) == set(kept) | gone
    entries = latest_entries(lake_root)
    for rel in gone:
        assert hashlib.sha256(restored[rel]).hexdigest() == entries[rel]["sha256"]


def test_the_real_trim_of_quotes_feeds_the_rebuild(lake_root, tmp_path):
    """Marketlake #794. The rebuild leaves out each quotes day the trim removed, as it does for
    chains. It keeps every bar, because this one run met each bar's chains on disk, and since
    marketlake #795 a bar is trimmed only on a run after its chains are gone."""
    tickers = ("QQQ", "SPY")
    client = _lake(lake_root, tickers=tickers, quotes=True)
    result = _job(lake_root, client, [])
    assert result.trim is not None
    gone = {_qrel(ticker, day) for ticker in tickers for day in OLD}
    assert gone <= set(result.trim.trimmed)
    with lake_lock(lake_root):
        nightly_upload(
            lake_root,
            TARGET,
            client=client,
            clock=ManualClock(_et(TONIGHT, 19, 0)),
            calendar=CALENDAR,
        )
    kept = _chains_files(lake_root, "quotes")
    assert kept and not gone & set(kept)
    windowed = restore_lake(
        tmp_path / "windowed", TARGET, client=client, skip_designed_absences=True
    )
    assert (windowed.restored, windowed.failures, windowed.trimmed_lost) == (True, [], [])
    assert _chains_files(tmp_path / "windowed", "quotes") == kept
    assert _chains_files(tmp_path / "windowed", "bars") == _chains_files(lake_root, "bars")


def test_the_real_trim_of_bars_feeds_the_rebuild(lake_root, tmp_path):
    """Marketlake #795. The first compaction trims the old chains and quotes and keeps their
    days' bars, since it met those chains on disk. The next run trims the bars, and the rebuild
    leaves them out as it does the chains, while the bar of a day inside the window comes
    back."""
    tickers = ("QQQ", "SPY")
    client = _lake(lake_root, tickers=tickers, quotes=True)
    bars = {
        f"bars/ticker={ticker}/freq=1d/date={day.isoformat()}.parquet"
        for ticker in tickers
        for day in OLD
    }
    first = _job(lake_root, client, [])
    assert first.trim is not None and not bars & set(first.trim.trimmed)
    second = _job(lake_root, client, [], clock=ManualClock(_et(TONIGHT, 16, 45)))
    assert second.trim is not None
    assert set(second.trim.trimmed) == bars
    with lake_lock(lake_root):
        nightly_upload(
            lake_root,
            TARGET,
            client=client,
            clock=ManualClock(_et(TONIGHT, 19, 0)),
            calendar=CALENDAR,
        )
    kept = _chains_files(lake_root, "bars")
    assert kept and not bars & set(kept)
    windowed = restore_lake(
        tmp_path / "windowed", TARGET, client=client, skip_designed_absences=True
    )
    assert (windowed.restored, windowed.failures, windowed.trimmed_lost) == (True, [], [])
    assert _chains_files(tmp_path / "windowed", "bars") == kept


# -- a window per surface, marketlake #953 ----------------------------------------------------
#
# In this calendar a window of 22 has its trim edge on 2026-07-29 and a window of 25 on
# 2026-07-24, so 2026-07-27 lies between the two edges and the ``OLD`` days past both. The floor
# of 22 keeps every window here from 22 to 29, and no guard can lower it.

BETWEEN = date(2026, 7, 27)
EDGE_25 = date(2026, 7, 24)
# A summary line on a host with ``lake_window_sessions`` alone, written out whole.
TODAYS_SUMMARY = "  trim     window=22 edge=2026-07-29 trimmed=2 restored=0 skipped=0 rot=0"


def _brel(ticker: str, day: date, freq: str = "1d") -> str:
    return f"bars/ticker={ticker}/freq={freq}/date={day.isoformat()}.parquet"


def _surface_lake(
    root: Path,
    *,
    tickers=("QQQ", "SPY"),
    bars_only=(),
    no_bar=(),
) -> FakeS3:
    """Chains, quotes and a daily bar per ticker on the ``OLD`` days, ``BETWEEN`` and yesterday.

    ``bars_only`` tickers, such as GLD, get only a daily bar on each of those days, and
    ``no_bar`` names ticker-days whose daily bar never landed. Everything is in the bucket.
    """
    lake = FixtureLake(root)
    for day in (*OLD, BETWEEN, YESTERDAY):
        for ticker in tickers:
            lake.with_partition(
                "chains", ticker, day, _chains(1, snap_ts=_snap(day, 0), ticker=ticker)
            )
            lake.with_partition(
                "quotes", ticker, day, _quotes(1, snap_ts=_snap(day, 0), ticker=ticker)
            )
            if (ticker, day) not in no_bar:
                lake.with_bars(ticker, "1d", day, pa.table({"close": [650.0]}))
        for ticker in bars_only:
            lake.with_bars(ticker, "1d", day, pa.table({"close": [180.0]}))
    lake.build()
    _checkpoint(root, YESTERDAY, dict.fromkeys(tickers, EDGE))
    client = FakeS3()
    first_upload(
        root, TARGET, client=client, clock=ManualClock(_et(YESTERDAY, 19, 0)), calendar=CALENDAR
    )
    client.calls.clear()
    return client


def _surface_job(root: Path, client: FakeS3, *, minute: int = 30, **surfaces):
    return _job(
        root,
        client,
        [],
        window=surfaces.pop("window", WINDOW),
        clock=ManualClock(_et(TONIGHT, 16, minute)),
        surfaces=surfaces,
    )


def _ran(result) -> list[str]:
    """The run's trim result, asserted to have run with no surface refused."""
    trim = result.trim
    assert trim is not None and trim.refused is None and trim.stopped is None
    assert trim.surface_refused == ()
    # A run that refused could not have dropped the chain past the chains edge.
    assert _rel("SPY", OLD[0]) in trim.trimmed
    return list(trim.trimmed)


def test_the_base_key_alone_prints_todays_summary_line_whole(lake_root):
    """Acceptance 2 on #953. Mutation this catches: a surface's pair appended when its window is
    the chains one, which changes the line every primary prints each night."""
    client = _lake(lake_root)

    result = _job(lake_root, client, [])

    assert result.trim is not None
    assert result.trim.render()[0] == TODAYS_SUMMARY
    assert result.render().splitlines()[-1] == TODAYS_SUMMARY


@pytest.mark.parametrize("value", ["'all'", "'ALL'", 25])
def test_a_bars_key_without_the_base_key_never_trims_and_prints_nothing_new(lake_root, value):
    """Acceptance 1. Only ``lake_window_sessions`` turns trimming on, so a host with a bars key
    alone, even one the judge would refuse, trims exactly what a host with no key does."""
    client = _lake(lake_root)
    events: list[str] = []

    result = _job(lake_root, client, events, window=None, surfaces={"window_sessions_bars": value})

    assert result.trim is None
    assert not any(event.startswith("get ") for event in events)
    assert "trim" not in result.render()


def test_an_absent_key_takes_the_base_window_never_the_chains_one(lake_root):
    """Acceptance 1. With chains 25 and the quotes and bars keys absent, quotes and GLD's bars,
    which have no chains, trim at the 22-session edge on 2026-07-27. SPY's chain that day is
    inside the chains window, so its bar waits for it."""
    client = _surface_lake(lake_root, bars_only=("GLD",))

    result = _surface_job(lake_root, client, window_sessions_chains=25)
    trimmed_rels = _ran(result)

    assert _qrel("SPY", BETWEEN) in trimmed_rels
    assert _brel("GLD", BETWEEN) in trimmed_rels
    assert (lake_root / _rel("SPY", BETWEEN)).exists()
    assert (lake_root / _brel("SPY", BETWEEN)).exists()
    assert result.trim.render()[0].endswith(" quotes=22/2026-07-29 bars=22/2026-07-29")


@pytest.mark.parametrize("bars", ["'all'", None], ids=["bars-kept", "bars-absent"])
def test_chains_22_quotes_25_drops_the_chain_and_keeps_the_quotes_day_between(lake_root, bars):
    surfaces = {"window_sessions_quotes": 25}
    if bars is not None:
        surfaces["window_sessions_bars"] = bars
    client = _surface_lake(lake_root)

    trimmed_rels = _ran(_surface_job(lake_root, client, **surfaces))
    result = _surface_job(lake_root, client, minute=45, **surfaces)

    assert _rel("SPY", BETWEEN) in trimmed_rels
    assert (lake_root / _qrel("SPY", BETWEEN)).exists()
    assert _qrel("SPY", OLD[0]) in trimmed_rels
    tail = " quotes=25/2026-07-24 bars=all" if bars is not None else " quotes=25/2026-07-24"
    assert result.trim is not None and result.trim.render()[0].endswith(tail)
    if bars is not None:
        assert all((lake_root / _brel(t, d)).exists() for t in ("QQQ", "SPY") for d in OLD)


def test_chains_25_bars_22_keeps_a_bar_whose_chain_is_inside_the_chains_window(lake_root):
    client = _surface_lake(lake_root)
    surfaces = {"window": 25, "window_sessions_bars": 22}

    first = _surface_job(lake_root, client, **surfaces)
    assert first.trim is not None and first.trim.surface_refused == ()
    assert _rel("SPY", OLD[0]) in first.trim.trimmed
    second = _surface_job(lake_root, client, minute=45, **surfaces)

    assert second.trim is not None
    assert _brel("SPY", OLD[0]) in second.trim.trimmed
    assert (lake_root / _rel("SPY", BETWEEN)).exists()
    assert (lake_root / _brel("SPY", BETWEEN)).exists()


def test_chains_25_quotes_22_keeps_a_quotes_day_past_its_edge_whose_bar_never_landed(lake_root):
    client = _surface_lake(lake_root, no_bar=(("SPY", BETWEEN),))

    first = _surface_job(lake_root, client, window_sessions_chains=25, window_sessions_quotes=22)
    trimmed_rels = _ran(first)

    assert (lake_root / _qrel("SPY", BETWEEN)).exists()
    assert _qrel("QQQ", BETWEEN) in trimmed_rels


def test_quotes_kept_keeps_every_quotes_partition(lake_root):
    client = _surface_lake(lake_root)

    result = _surface_job(lake_root, client, window_sessions_quotes="'all'")
    trimmed_rels = _ran(result)

    assert not any(rel.startswith("quotes/") for rel in trimmed_rels)
    assert all((lake_root / _qrel(t, d)).exists() for t in ("QQQ", "SPY") for d in (*OLD, BETWEEN))
    assert result.trim.render()[0].endswith(" quotes=all")


def test_chains_22_bars_25_keeps_bars_between_the_edges_with_or_without_a_chain(lake_root):
    """GLD has no chains, and SPY's chain between the edges goes on the first run, so the bars
    window alone keeps their bars there on the second. Bars past the 25-session edge go."""
    client = _surface_lake(lake_root, bars_only=("GLD",))

    first = _surface_job(lake_root, client, window_sessions_bars=25)
    trimmed_rels = _ran(first)
    second = _surface_job(lake_root, client, minute=45, window_sessions_bars=25)

    assert _rel("SPY", BETWEEN) in trimmed_rels
    assert _brel("GLD", OLD[0]) in trimmed_rels
    assert second.trim is not None and _brel("SPY", OLD[0]) in second.trim.trimmed
    assert (lake_root / _brel("GLD", BETWEEN)).exists()
    assert (lake_root / _brel("SPY", BETWEEN)).exists()
    assert second.trim.render()[0].endswith(f" bars=25/{EDGE_25.isoformat()}")


def test_bars_kept_leaves_every_bar_of_a_ticker_with_no_chains_for_load_bars(lake_root):
    """Acceptance 3. GLD's bars of both frequencies sit on days the chains trim reaches, and a
    run without the bars key would trim them, since GLD has no chain to wait for. They are
    written by the sweep's own writer beside the schema-version ledger, so ``load_bars`` reads
    them back, which the trim tests' one-column bars could not pass."""
    from lake.loader import load_bars
    from lake.manifest import record_partition
    from tests.component.test_load_bars import _ledger_table
    from tests.support.bars import write_bars

    lake = FixtureLake(lake_root).with_reference("schema_versions", _ledger_table())
    for day in (*OLD, YESTERDAY):
        lake.with_partition("chains", "SPY", day, _chains(1, snap_ts=_snap(day, 0)))
        lake.with_partition("quotes", "SPY", day, _quotes(1, snap_ts=_snap(day, 0)))
        lake.with_bars("SPY", "1d", day, pa.table({"close": [650.0]}))
    root = lake.build()
    gld = []
    for day in (*OLD, YESTERDAY):
        for freq in ("1d", "1m"):
            path = write_bars(root, "GLD", freq, day.isoformat(), [180.0], instrument_id=7)
            gld.append(path.relative_to(root).as_posix())
    with lake_lock(root):
        for rel in gld:
            record_partition(root, rel, source="bars", rows=1, fetched_at=None)
    _checkpoint(root, YESTERDAY, {"SPY": EDGE})
    client = FakeS3()
    first_upload(
        root, TARGET, client=client, clock=ManualClock(_et(YESTERDAY, 19, 0)), calendar=CALENDAR
    )
    assert all(client.objects.get(TARGET.key(rel)) is not None for rel in gld)

    result = _surface_job(root, client, window_sessions_bars="'all'")
    trimmed_rels = _ran(result)

    assert {_rel("SPY", day) for day in OLD} | {_qrel("SPY", day) for day in OLD} <= set(
        trimmed_rels
    )
    assert not any(rel.startswith("bars/") for rel in trimmed_rels)
    assert all((root / rel).exists() for rel in gld)
    for freq in ("1d", "1m"):
        table = load_bars("GLD", freq, lake_root=root)
        assert table.num_rows == len((*OLD, YESTERDAY))
    assert result.trim.render()[0].endswith(" bars=all")


HEX = int("f" * 4000, 16)


@pytest.mark.parametrize("surface", ["quotes", "bars"])
@pytest.mark.parametrize(
    ("value", "why"),
    [
        ("'ALL'", "{key} is not a whole number of sessions or all"),
        (
            400,
            "the window edge could not be counted: only 30 of 401 sessions in the 1686 days "
            "up to 2026-08-28, so the window is longer than the search reaches",
        ),
        (HEX, "the window edge could not be counted (ValueError)"),
    ],
    ids=["judge-refused", "edge-not-found", "edge-raises"],
)
def test_a_refused_quotes_or_bars_window_keeps_the_chains_trim_running(
    lake_root, surface, value, why
):
    """Acceptance 5. Through ``compact``, since a ``_trim_step`` that forgets the judge's refusals
    is seen only there. A hex window loads as an ``int`` and passes the judge, and the count
    then raises ``ValueError`` formatting its own message."""
    key = f"lake_window_sessions_{surface}"
    client = _surface_lake(lake_root, bars_only=("GLD",))

    result = _surface_job(lake_root, client, **{f"window_sessions_{surface}": value})

    trim = result.trim
    assert trim is not None and trim.refused is None and trim.stopped is None
    expected = why.format(key=key)
    assert trim.surface_refused == ((surface, expected),)
    assert f"  trim     {surface} refused: {expected}" in result.render().splitlines()
    assert _rel("SPY", OLD[0]) in trim.trimmed and _rel("SPY", BETWEEN) in trim.trimmed
    assert not any(rel.startswith(f"{surface}/") for rel in trim.trimmed)


def test_a_refused_base_key_refuses_the_whole_run_beside_every_good_surface_key(lake_root):
    client = _surface_lake(lake_root)

    result = _surface_job(
        lake_root,
        client,
        window=21,
        window_sessions_chains=22,
        window_sessions_quotes=22,
        window_sessions_bars="'all'",
    )

    assert result.trim is not None and result.trim.trimmed == ()
    assert result.trim.render() == [
        "  trim     refused: lake_window_sessions is under the floor of 22 sessions that the "
        "readers of sealed chains and quotes need, which are the battery's trailing median, the "
        "History panel and the disk runway. Raise it in config/vm.yaml"
    ]


def test_a_refused_chains_key_refuses_the_whole_run(lake_root):
    client = _surface_lake(lake_root)

    result = _surface_job(lake_root, client, window_sessions_chains="'all'")

    assert result.trim is not None and result.trim.trimmed == ()
    assert result.trim.refused == (
        "lake_window_sessions_chains keeps every session, which only lake_window_sessions_bars "
        "and lake_window_sessions_quotes may, because the split checkpoint is counted from the "
        "chains window. Set a whole number in config/vm.yaml"
    )


def test_the_bars_key_in_config_yaml_reaches_the_trim(lake_root, tmp_path, monkeypatch, capsys):
    """Acceptance 9. ``main`` hands each per-surface key from ``config.yaml`` to ``compact``."""
    client = _surface_lake(lake_root, bars_only=("GLD",))
    config = _evening_config(tmp_path, lake_root)
    config.write_text(config.read_text() + "lake_window_sessions_bars: all\n")
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    monkeypatch.setattr("lake.runner.UrllibPinger", lambda: FakePinger())

    code = compact_module.main(
        ["--config", str(config), "--plan", str(tmp_path / "chain_plan.json")],
        clock=ManualClock(_et(TONIGHT, 16, 30)),
        calendar=CALENDAR,
    )

    assert code == 0
    summary = [
        line for line in capsys.readouterr().out.splitlines() if line.startswith("  trim     ")
    ][0]
    assert summary.startswith("  trim     window=22 edge=2026-07-29 ")
    assert summary.endswith(" bars=all")
    assert (lake_root / _brel("GLD", OLD[0])).exists()
    assert not (lake_root / _rel("SPY", OLD[0])).exists()


def test_the_chains_and_quotes_keys_in_config_yaml_reach_the_trim(
    lake_root, tmp_path, monkeypatch, capsys
):
    """Acceptance 9. ``main`` hands the chains and quotes keys to ``compact`` too, so the summary
    shows the chains key's window and the quotes key's ``all``."""
    client = _surface_lake(lake_root, bars_only=("GLD",))
    config = _evening_config(tmp_path, lake_root)
    config.write_text(
        config.read_text() + "lake_window_sessions_chains: 25\nlake_window_sessions_quotes: all\n"
    )
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    monkeypatch.setattr("lake.runner.UrllibPinger", lambda: FakePinger())

    code = compact_module.main(
        ["--config", str(config), "--plan", str(tmp_path / "chain_plan.json")],
        clock=ManualClock(_et(TONIGHT, 16, 30)),
        calendar=CALENDAR,
    )

    assert code == 0
    summary = [
        line for line in capsys.readouterr().out.splitlines() if line.startswith("  trim     ")
    ][0]
    assert summary.startswith("  trim     window=25 ")
    assert " quotes=all" in summary
