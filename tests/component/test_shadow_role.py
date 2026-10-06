"""The ``role`` key: a shadow host records its pings and pages and backs nothing up.

A second machine runs the full daemon beside the primary before a cutover, and it must not
feed the owner's alarms. ``lake.outbox`` builds every sender in the package, and under
``role: shadow`` it hands each ``main`` two recorders that append one line per call to
``journal/outbox/date=D.jsonl``.

The tests below follow the Verification list on marketlake #647, one block per item.

The ``main`` twins patch no sender. ``tests/conftest.py``'s network guard raises a
``BaseException`` that escapes every ``except`` on the send paths, and its subprocess guard
fails any ``rsync``. So a live sender or a backup the change missed fails a twin by itself,
which a twin that patched its senders would hide. Each twin reads back the exact lines it
expects, so a recorder that drops a field fails it too.
"""

from __future__ import annotations

import copy
import json
import pickle
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from lake import alert, bucket, compact, daemon, outbox, runner, sweep
from lake import control_plane as cp
from lake import probe_calendar as probe_module
from lake.alert import Message
from lake.capture import CycleResult, SegmentOutcome
from lake.config import ROLE_ABSENT, Config, load_config
from lake.control_plane import (
    BACKUP_SCRUB_SKIPPED,
    CALENDAR_PROBE_SLUG,
    COMPACTION_SLUG,
    EOD_SWEEP_SLUG,
    PRE_OPEN_SLUG,
    SUNDAY_DAEMON_DOWN_EVENT,
    SUNDAY_DAEMON_DOWN_TITLE,
    SUNDAY_SLUG,
)
from lake.journal import ROW_KIND_DATA
from lake.metadata import stamp_assertion_pid
from lake.paths import LakePaths
from lake.probe_calendar import PAGE_TITLE
from lake.runner import BACKUP_SKIPPED, SLICE1_RUNNER_SLUG
from lake.vendor import VendorResponse
from tests.component.test_eod_sweep import (
    EVENING,
    MONDAY,
    NEXT_MONDAY,
    _CountingVendorSource,
    _lake,
    _RecordingSetter,
    _schedule_text,
)
from tests.support.backup import FakeBackup
from tests.support.calendar import FakeCalendar, SessionTimes, et, weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import NTFY_TOPIC, PING_KEY, write_config
from tests.support.lake import FixtureLake
from tests.support.pinger import FakePinger
from tests.support.transport import FakeTransport

SATURDAY = et(2026, 9, 5, 12, 0)
SUNDAY_20 = et(2026, 8, 30, 20, 0)
FRESH_MINT = et(2026, 8, 30, 19, 30)
REPEAT_ONLY = "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n"


def _outbox(root: Path, day: date) -> list[dict]:
    """Every line in one day's outbox file, parsed."""
    path = LakePaths(root).outbox_path(day)
    return [json.loads(line) for line in path.read_text().splitlines()]


def _ping(at: datetime, process: str, check: str | None) -> dict:
    return {
        "v": outbox.OUTBOX_FORMAT_VERSION,
        "at": at.astimezone(UTC).isoformat(),
        "process": process,
        "kind": "ping",
        "check": check,
    }


def _page(at: datetime, process: str, event: str, title: str, priority: int) -> dict:
    return {
        "v": outbox.OUTBOX_FORMAT_VERSION,
        "at": at.astimezone(UTC).isoformat(),
        "process": process,
        "kind": "page",
        "event": event,
        "title": title,
        "priority": priority,
    }


def _config(root: Path, *, key: str = PING_KEY, role: object = "shadow") -> Config:
    return Config.from_mapping(
        {
            "lake_root": str(root),
            "backup_target": str(root.parent / "ssd"),
            "healthchecks_ping_key": key,
            "ntfy_topic": NTFY_TOPIC,
            "schwab_api_key": "api-key",
            "schwab_app_secret": "app-secret",
            "role": role,
        }
    )


# -- 1. the role cases ------------------------------------------------------------------


@pytest.mark.parametrize(
    ("written", "role", "named"),
    [
        pytest.param(None, outbox.PRIMARY, None, id="absent"),
        pytest.param("primary", outbox.PRIMARY, None, id="primary"),
        pytest.param("shadow", outbox.SHADOW, None, id="shadow"),
        pytest.param("shadw", outbox.SHADOW, "'shadw'", id="misspelled"),
        pytest.param("", outbox.SHADOW, "'None'", id="empty-is-null"),
        pytest.param("off", outbox.SHADOW, "'False'", id="off-is-false"),
    ],
)
def test_the_role_key_reads_as_four_cases(tmp_path, capsys, written, role, named):
    root = tmp_path / "lake"
    root.mkdir()
    config = load_config(write_config(tmp_path, root, role=written))

    sends = outbox.senders(config, process="probe", clock=ManualClock(SATURDAY))

    assert sends.role == role
    if role == outbox.PRIMARY:
        assert isinstance(sends.transport, alert.NtfyTransport)
        assert isinstance(sends.pinger, runner.UrllibPinger)
    else:
        assert isinstance(sends.transport, outbox.RecordingTransport)
        assert isinstance(sends.pinger, outbox.RecordingPinger)
    err = capsys.readouterr().err
    if named is None:
        assert err == ""
    else:
        # One line, naming the process, the key and the value exactly as YAML read it.
        assert err.count("\n") == 1
        assert err.startswith(f"probe: role {named} is neither")


def test_the_loader_keeps_an_empty_role_apart_from_an_absent_one(tmp_path):
    root = tmp_path / "lake"
    absent = load_config(write_config(tmp_path, root))
    empty = load_config(write_config(tmp_path, root, role=""))
    assert empty.role == "None"
    assert absent.role is ROLE_ABSENT


@pytest.mark.parametrize(
    "duplicate",
    [
        pytest.param(copy.copy, id="copy"),
        pytest.param(copy.deepcopy, id="deepcopy"),
        pytest.param(lambda config: pickle.loads(pickle.dumps(config)), id="pickle"),
    ],
)
def test_a_copied_config_without_the_key_still_reads_as_primary(tmp_path, capsys, duplicate):
    """``lake.outbox`` compares the absent marker by identity, so a copy must keep it."""
    root = tmp_path / "lake"
    root.mkdir()
    config = duplicate(load_config(write_config(tmp_path, root)))

    assert outbox.role_of(config) == (outbox.PRIMARY, None)
    assert capsys.readouterr().err == ""


def test_a_list_role_leaves_the_config_hashable_and_reads_as_shadow(tmp_path, capsys):
    """The loader stores a non-string value's ``repr``, so the frozen config stays hashable."""
    root = tmp_path / "lake"
    root.mkdir()
    config = load_config(write_config(tmp_path, root, role="[shadow]"))

    hash(config)
    sends = outbox.senders(config, process="probe", clock=ManualClock(SATURDAY))
    assert sends.role == outbox.SHADOW
    assert capsys.readouterr().err.startswith("probe: role \"['shadow']\" is neither")


# -- 2. no ping key in the file ---------------------------------------------------------

DISTINCT_KEY = "k3y-Distinct-7f9a"


def test_a_recorded_ping_names_the_check_and_never_the_key(lake_root):
    config = _config(lake_root, key=DISTINCT_KEY)
    clock = ManualClock(SATURDAY)
    pinger = outbox.senders(config, process="sweep", clock=clock).pinger

    pinger.ping(config.healthchecks_url(EOD_SWEEP_SLUG))
    # A suffix after the slug stays part of the check, so a future /fail reads whole.
    pinger.ping(config.healthchecks_url(f"{COMPACTION_SLUG}/fail"))
    # A URL that does not start with the known prefix records no URL text at all.
    pinger.ping(f"https://elsewhere.test/{DISTINCT_KEY}/capture")

    assert _outbox(lake_root, SATURDAY.date()) == [
        _ping(SATURDAY, "sweep", EOD_SWEEP_SLUG),
        _ping(SATURDAY, "sweep", f"{COMPACTION_SLUG}/fail"),
        _ping(SATURDAY, "sweep", None),
    ]
    raw = LakePaths(lake_root).outbox_path(SATURDAY.date()).read_bytes()
    assert DISTINCT_KEY.encode() not in raw
    assert b"elsewhere.test" not in raw
    assert b"hc-ping.com" not in raw


def test_a_recorded_page_keeps_no_body(lake_root):
    transport = outbox.senders(
        _config(lake_root), process="daemon", clock=ManualClock(SATURDAY)
    ).transport

    transport.send(Message(event="capture_down", title="Capture down", body="body text"))

    assert _outbox(lake_root, SATURDAY.date()) == [
        _page(SATURDAY, "daemon", "capture_down", "Capture down", 5)
    ]
    assert b"body text" not in LakePaths(lake_root).outbox_path(SATURDAY.date()).read_bytes()


# -- 3. the Eastern date ----------------------------------------------------------------


def test_a_line_after_eight_in_the_evening_lands_in_that_eastern_days_file(lake_root):
    # 23:30 on a Sunday in Eastern time is already Monday in UTC. The Sunday reminder runs
    # then, and it must share a file with the rest of that evening.
    late = et(2026, 9, 6, 23, 30)
    pinger = outbox.senders(_config(lake_root), process="sunday", clock=ManualClock(late)).pinger

    pinger.ping(_config(lake_root).healthchecks_url(SUNDAY_SLUG))

    assert _outbox(lake_root, date(2026, 9, 6)) == [_ping(late, "sunday", SUNDAY_SLUG)]
    assert not LakePaths(lake_root).outbox_path(date(2026, 9, 7)).exists()


# -- 4. a missing lake root -------------------------------------------------------------


def test_a_recorder_on_a_missing_lake_root_raises_and_creates_nothing(tmp_path):
    root = tmp_path / "unmounted" / "lake"
    sends = outbox.senders(_config(root), process="sunday", clock=ManualClock(SUNDAY_20))

    with pytest.raises(FileNotFoundError):
        sends.pinger.ping(_config(root).healthchecks_url(SUNDAY_SLUG))
    with pytest.raises(FileNotFoundError):
        sends.transport.send(Message(event="e", title="t", body="b"))

    assert not root.exists()
    assert not root.parent.exists()


# -- 5. only OSError --------------------------------------------------------------------


class _BrokenClock(ManualClock):
    def now(self) -> datetime:
        raise ValueError("no time today")


def test_a_recorder_raises_only_oserror(lake_root):
    sends = outbox.senders(_config(lake_root), process="sweep", clock=_BrokenClock(SATURDAY))
    with pytest.raises(OSError) as pinged:
        sends.pinger.ping(_config(lake_root).healthchecks_url(EOD_SWEEP_SLUG))
    assert type(pinged.value) is OSError
    assert isinstance(pinged.value.__cause__, ValueError)

    sends = outbox.senders(_config(lake_root), process="sweep", clock=ManualClock(SATURDAY))
    with pytest.raises(OSError) as sent:
        sends.transport.send(Message(event="e", title="t", body="b", priority="high"))  # type: ignore[arg-type]
    assert type(sent.value) is OSError
    assert isinstance(sent.value.__cause__, ValueError)


# -- 6 and 8. the seven mains, with no sender patched -----------------------------------


def test_the_daemon_twin_records_through_the_senders_it_was_handed(tmp_path, monkeypatch, capsys):
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root, role="shadow")
    seen: dict = {}
    monkeypatch.setattr(daemon, "run_loop_from_config", lambda **kwargs: seen.update(kwargs))
    monkeypatch.setattr(daemon, "SystemClock", lambda: ManualClock(SATURDAY))

    assert daemon.main(["--config", str(config)]) == 0
    seen["pinger"].ping(f"https://hc-ping.com/{PING_KEY}/capture")
    seen["transport"].send(Message(event="capture_down", title="Capture down", body="b"))

    assert _outbox(lake_root, SATURDAY.date()) == [
        _ping(SATURDAY, "daemon", "capture"),
        _page(SATURDAY, "daemon", "capture_down", "Capture down", 5),
    ]
    assert capsys.readouterr().err == "daemon: role=shadow\n"


def test_the_compaction_twin_records_its_ping_and_skips_the_backup(tmp_path, capsys):
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root, role="shadow")
    now = et(2026, 8, 24, 16, 30)
    # A line already in the day's outbox, as the daemon would have left it. Compaction
    # walks ``journal/`` and must leave the file alone.
    earlier = _ping(now, "daemon", "capture")
    path = LakePaths(lake_root).outbox_path(now.date())
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(earlier) + "\n")

    code = compact.main(
        ["--config", str(config), "--plan", str(tmp_path / "chain_plan.json")],
        clock=ManualClock(now),
        calendar=FakeCalendar(
            {now.date(): SessionTimes(open=et(2026, 8, 24, 9, 30), close=et(2026, 8, 24, 16, 0))}
        ),
    )

    assert code == 0
    assert _outbox(lake_root, now.date()) == [earlier, _ping(now, "compact", COMPACTION_SLUG)]
    out = capsys.readouterr().out
    assert "backed_up=False pinged=True" in out
    assert BACKUP_SKIPPED in out


def test_the_sweep_twin_records_its_ping_and_digest(fixture_lake, tmp_path, monkeypatch):
    root = _lake(fixture_lake)
    config = write_config(tmp_path, lake_root=root, role="shadow")
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("SPY:\n  options: true\n  bars:\n  - 1d\n")
    monkeypatch.setattr(sweep, "ExchangeCalendar", lambda: weekday_sessions(MONDAY, NEXT_MONDAY))

    code = sweep.main(
        ["--config", str(config), "--tickers", str(tickers)],
        clock=ManualClock(EVENING),
        vendor_source=_CountingVendorSource(),
        schedule_setter=_RecordingSetter(),
        schedule_reader=lambda: _schedule_text(),
    )

    assert code == 0
    assert _outbox(root, EVENING.date()) == [
        _ping(EVENING, "sweep", EOD_SWEEP_SLUG),
        _page(
            EVENING,
            "sweep",
            sweep.NIGHTLY_EVENT,
            f"Nightly {EVENING.date().isoformat()}",
            sweep.NIGHTLY_PRIORITY,
        ),
    ]


def test_the_self_check_twin_records_its_ping(tmp_path, monkeypatch):
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root, role="shadow")
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
    # Saturday, so nothing is owed and the ping is the whole run.
    monkeypatch.setattr(cp, "_system_clock", lambda: ManualClock(start=SATURDAY))

    assert cp.main(["self-check", "--config", str(config)]) == 0

    assert _outbox(lake_root, SATURDAY.date()) == [_ping(SATURDAY, "self-check", PRE_OPEN_SLUG)]


def _excluded(paths) -> str:
    """A reader reporting every path already excluded, the healthy Time Machine state."""
    return "".join(f"[Excluded]\t{p}\n" for p in paths)


def test_the_sunday_twin_skips_the_backup_scrub_and_pings(tmp_path, monkeypatch, capsys):
    lake_root = FixtureLake(tmp_path / "lake").with_chains("SPY", date(2026, 8, 28)).build()
    # The target exists and holds nothing a sync would have left, so a scrub of it would
    # find no manifest, withhold the ping and retry until the deadline.
    config = write_config(tmp_path, lake_root, backup_target=tmp_path / "ssd", role="shadow")
    token = tmp_path / "token.json"
    token.write_text(
        json.dumps({"creation_timestamp": FRESH_MINT.timestamp(), "token": {"x": "never-read"}})
    )
    # A line from earlier in the day. The lake scrub walks the tree and must not count it.
    earlier = _ping(SUNDAY_20, "daemon", "capture")
    path = LakePaths(lake_root).outbox_path(SUNDAY_20.date())
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(earlier) + "\n")
    monkeypatch.setattr(cp, "read_pmset_schedule", lambda: REPEAT_ONLY)
    monkeypatch.setattr(cp, "token_canary", lambda **kwargs: lambda: True)
    monkeypatch.setattr(cp, "read_exclusions", _excluded)
    # The daemon answers down, so the evening's one page goes through the transport too.
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: False)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)

    code = cp.main(
        ["sunday", "--config", str(config), "--token", str(token)],
        clock=ManualClock(start=SUNDAY_20),
        calendar=weekday_sessions(date(2026, 8, 31), date(2026, 9, 7)),
    )

    printed = capsys.readouterr().out
    assert code == 0, printed
    assert _outbox(lake_root, SUNDAY_20.date()) == [
        earlier,
        _ping(SUNDAY_20, "sunday", SUNDAY_SLUG),
        _page(SUNDAY_20, "sunday", SUNDAY_DAEMON_DOWN_EVENT, SUNDAY_DAEMON_DOWN_TITLE, 5),
    ]
    assert f"sunday: report: {BACKUP_SCRUB_SKIPPED}" in printed
    assert "attempts=1 pinged=True" in printed
    assert "scrub failed" not in printed


def test_the_calendar_probe_twin_records_its_ping_and_page(tmp_path, monkeypatch):
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root, role="shadow")
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("SPY: {options: true, chain_cadence: 1m}\n")
    token = tmp_path / "token.json"
    token.write_text(json.dumps({"token": {"x": "never-read"}}))
    # A Saturday the market is quoting through, so the probe pages as well as pings.
    now = et(2026, 9, 5, 9, 35)
    stamp = int(now.timestamp() * 1000)

    class _Vendor:
        @staticmethod
        def from_token(path, *, api_key, app_secret, clock=None):
            return _Vendor()

        def get_quotes(self, symbols):
            return VendorResponse(
                status=200, body={sym: {"quote": {"quoteTime": stamp}} for sym in symbols}
            )

    monkeypatch.setattr("lake.schwab.SchwabVendor", _Vendor)
    monkeypatch.setattr("lake.clock.SystemClock", lambda: ManualClock(now))

    code = probe_module.main(
        ["--config", str(config), "--tickers", str(tickers), "--token", str(token)]
    )

    assert code == 1
    assert _outbox(lake_root, now.date()) == [
        _ping(now, "probe-calendar", CALENDAR_PROBE_SLUG),
        _page(now, "probe-calendar", "calendar_wrong", PAGE_TITLE, 5),
    ]


def test_the_test_push_twin_refuses_and_records_nothing(tmp_path, capsys):
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root, role="shadow")

    code = alert.main(["--test-push", "--config", str(config)], clock=ManualClock(SATURDAY))

    assert code == 2
    assert capsys.readouterr().err == alert.TEST_PUSH_SHADOW + "\n"
    assert not (lake_root / "journal").exists()
    assert not (lake_root / "reports").exists()


def _captured() -> CycleResult:
    snap = datetime(2026, 9, 2, 20, 0, tzinfo=UTC)
    return CycleResult(
        snap_ts=snap,
        segments=(
            SegmentOutcome(
                surface="quotes",
                ticker="SPY",
                path=Path("/lake/journal/quotes-SPY.arrows"),
                partition="journal/quotes/SPY.arrows",
                row_kind=ROW_KIND_DATA,
                rows=1,
                error_class=None,
                fetched_at=snap.isoformat(),
                data_rows=1,
            ),
        ),
    )


def test_the_runner_twin_records_its_ping_and_skips_the_backup(tmp_path, monkeypatch, capsys):
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root, role="shadow")
    monkeypatch.setattr(runner, "run_cycle_from_config", lambda **kwargs: _captured())
    monkeypatch.setattr(runner, "SystemClock", lambda: ManualClock(SATURDAY))

    assert runner.main(["run", "--config", str(config)]) == 0

    assert _outbox(lake_root, SATURDAY.date()) == [_ping(SATURDAY, "runner", SLICE1_RUNNER_SLUG)]
    out = capsys.readouterr().out
    assert f"slice-1 run: {BACKUP_SKIPPED}" in out
    assert "pinged=True backed_up=False" in out


# -- 7. the skip is visible -------------------------------------------------------------


def test_compaction_with_no_backup_runner_says_so(lake_root):
    now = et(2026, 8, 24, 16, 30)
    pinger = FakePinger()

    result = compact.compact(
        lake_root,
        clock=ManualClock(now),
        calendar=FakeCalendar(
            {now.date(): SessionTimes(open=et(2026, 8, 24, 9, 30), close=et(2026, 8, 24, 16, 0))}
        ),
        backup=None,
        backup_target=lake_root.parent / "never-synced",
        pinger=pinger,
        ping_url="https://hc-ping.com/k/compaction",
    )

    assert result.backed_up is False
    assert result.pinged is True
    assert pinger.urls == ["https://hc-ping.com/k/compaction"]
    assert result.render().splitlines()[1] == f"  {BACKUP_SKIPPED}"


def test_a_compaction_that_synced_prints_no_skip_line(lake_root):
    now = et(2026, 8, 24, 16, 30)
    backup = FakeBackup()
    result = compact.compact(
        lake_root,
        clock=ManualClock(now),
        calendar=FakeCalendar(
            {now.date(): SessionTimes(open=et(2026, 8, 24, 9, 30), close=et(2026, 8, 24, 16, 0))}
        ),
        backup=backup,
        backup_target=lake_root.parent / "ssd",
    )
    assert result.backed_up is True
    assert BACKUP_SKIPPED not in result.render()


def test_the_runner_with_no_backup_runner_pings_and_reports_no_backup():
    pinger = FakePinger()
    outcome = runner.run_once(
        _captured,
        pinger=pinger,
        ping_url="https://hc-ping.com/k/slice1-capture",
        backup=None,
        lake_root=Path("/lake"),
        backup_target=Path("/ssd"),
    )
    assert outcome.succeeded is True
    assert outcome.backed_up is False
    assert outcome.pinged is True


def test_the_sunday_job_without_a_target_reports_the_skip_and_no_problem(tmp_path):
    lake_root = FixtureLake(tmp_path / "lake").with_chains("SPY", date(2026, 8, 28)).build()
    outcome = cp.sunday_maintenance(
        lake_root=lake_root,
        backup_target=None,
        now=SUNDAY_20,
        calendar=weekday_sessions(date(2026, 8, 31), date(2026, 9, 7)),
        schedule_reader=lambda: REPEAT_ONLY,
        pinger=FakePinger(),
        ping_url="https://hc-ping.com/k/sunday",
        canary=lambda: True,
        mint=FRESH_MINT,
    )
    assert outcome.backup is None
    assert outcome.problems == ()
    assert BACKUP_SCRUB_SKIPPED in outcome.report
    assert outcome.pinged is True


# -- 9. the default ---------------------------------------------------------------------


def test_a_config_without_the_key_builds_the_live_pair_and_says_primary(
    tmp_path, monkeypatch, capsys
):
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    seen: dict = {}
    monkeypatch.setattr(daemon, "run_loop_from_config", lambda **kwargs: seen.update(kwargs))

    assert daemon.main(["--config", str(config)]) == 0

    assert isinstance(seen["transport"], alert.NtfyTransport)
    assert isinstance(seen["pinger"], runner.UrllibPinger)
    assert capsys.readouterr().err == "daemon: role=primary\n"
    assert not (lake_root / "journal").exists()


def test_a_fake_transport_patched_on_alert_reaches_a_primary_main(tmp_path, monkeypatch):
    # The fifty repointed patches rest on this: ``outbox`` looks the class up on
    # ``lake.alert`` at call time, so a patch there reaches every ``main``.
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    transport = FakeTransport()
    monkeypatch.setattr("lake.alert.NtfyTransport", lambda topic: transport)

    assert alert.main(["--test-push", "--config", str(config)], clock=ManualClock(SATURDAY)) == 0
    assert [m.event for m in transport.messages] == [alert.TEST_PUSH_EVENT]


# -- the bucket form of the backup ------------------------------------------------------

S3_TARGET = "s3://lake-backup/lake"


def _bucket_config(tmp_path: Path, lake_root: Path) -> Path:
    """A shadow config naming a bucket target and carrying no bucket keys at all.

    The missing keys are the point. A primary given this config fails to build a client,
    which ``control_plane.main`` turns into a backup problem that withholds the ``sunday``
    ping and makes no network call. So the network guard cannot see that branch, and only
    an outcome that pinged shows the shadow skipped it.
    """
    config = write_config(tmp_path, lake_root, role="shadow")
    text = config.read_text().replace(
        f"backup_target: {tmp_path / 'ssd'}", f"backup_target: {S3_TARGET}"
    )
    config.write_text(text)
    return config


def test_the_sunday_twin_with_a_bucket_target_skips_the_whole_backup_step(
    tmp_path, monkeypatch, capsys
):
    lake_root = FixtureLake(tmp_path / "lake").with_chains("SPY", date(2026, 8, 28)).build()
    config = _bucket_config(tmp_path, lake_root)
    token = tmp_path / "token.json"
    token.write_text(
        json.dumps({"creation_timestamp": FRESH_MINT.timestamp(), "token": {"x": "never-read"}})
    )
    monkeypatch.setattr(cp, "read_pmset_schedule", lambda: REPEAT_ONLY)
    monkeypatch.setattr(cp, "token_canary", lambda **kwargs: lambda: True)
    monkeypatch.setattr(cp, "read_exclusions", _excluded)
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
    # A stamped pid keeps the daemon-liveness page out, so the ping is the one line.
    stamp_assertion_pid(lake_root, pid=4242)
    # A shadow builds no bucket client at all. With these settings a client could not be
    # built anyway, and the skip discards the result, so only the call itself shows it.
    monkeypatch.setattr(bucket, "connect", lambda *args: pytest.fail("a shadow built a client"))

    code = cp.main(
        ["sunday", "--config", str(config), "--token", str(token)],
        clock=ManualClock(start=SUNDAY_20),
        calendar=weekday_sessions(date(2026, 8, 31), date(2026, 9, 7)),
    )

    printed = capsys.readouterr().out
    assert code == 0, printed
    assert _outbox(lake_root, SUNDAY_20.date()) == [_ping(SUNDAY_20, "sunday", SUNDAY_SLUG)]
    assert f"sunday: report: {BACKUP_SCRUB_SKIPPED}" in printed
    assert "attempts=1 pinged=True" in printed


def test_the_compaction_twin_with_a_bucket_target_uploads_nothing(tmp_path, capsys):
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = _bucket_config(tmp_path, lake_root)
    now = et(2026, 8, 24, 16, 30)

    code = compact.main(
        ["--config", str(config), "--plan", str(tmp_path / "chain_plan.json")],
        clock=ManualClock(now),
        calendar=FakeCalendar(
            {now.date(): SessionTimes(open=et(2026, 8, 24, 9, 30), close=et(2026, 8, 24, 16, 0))}
        ),
    )

    assert code == 0
    assert _outbox(lake_root, now.date()) == [_ping(now, "compact", COMPACTION_SLUG)]
    out = capsys.readouterr().out
    assert "backed_up=False pinged=True" in out
    assert BACKUP_SKIPPED in out


@pytest.mark.parametrize(
    "argv",
    [
        pytest.param(["first-upload"], id="first-upload"),
        pytest.param(["live-check", "--target", "s3://probe-bucket/probe"], id="live-check"),
    ],
)
def test_the_bucket_commands_refuse_on_a_shadow_host(tmp_path, monkeypatch, capsys, argv):
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = _bucket_config(tmp_path, lake_root)
    monkeypatch.setattr(bucket, "connect", lambda *args: pytest.fail("a shadow built a client"))

    code = bucket.main([*argv, "--config", str(config)], clock=ManualClock(SATURDAY))

    assert code == 2
    assert capsys.readouterr().err == f"{argv[0]}: {bucket.BUCKET_SHADOW}\n"
    assert not (lake_root / "journal").exists()
