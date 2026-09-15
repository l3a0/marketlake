"""Every live producer escalates a refused ping, and every one of them is wired for it.

Five jobs ping a health check. A slug with no row is refused on every one of their runs
forever, and the only symptom is silence, which is the symptom the check exists to
report. So the page has to come from the producer that made the ping, and each of the
five is covered here twice.

1. The producer escalates. Given a publisher and a ping healthchecks refuses, it pages,
   and the page names the slug it pinged.
2. The composition root hands it a real publisher. A seam that only tests reach is a
   seam that silently goes unwired, and four of these five gained the seam in this
   change.

The dead-man's root is the daemon's own wiring, so its case lives beside the other
loop bindings in ``test_daemon_wiring.py``.

Nothing here reaches the network, a subprocess, or a phone. Every past-process producer
each ``main`` builds is replaced by name, which is how these drive the real wiring with
nothing live at the leaves.
"""

from __future__ import annotations

import json
import urllib.error
from collections.abc import Sequence
from datetime import date, datetime

import pytest

from lake import compact as compact_module
from lake import control_plane as cp
from lake import probe_calendar as probe_module
from lake.alert import Message
from lake.compact import COMPACTION_SLUG, compact
from lake.control_plane import CALENDAR_PROBE_SLUG, PRE_OPEN_SLUG, SUNDAY_SLUG
from lake.deadman import CAPTURE_SLUG, DeadMan
from lake.probe_calendar import ProbeResult, report
from lake.runner import PING_REFUSED_EVENT, SlugEscalation
from lake.session import SessionClock
from tests.support.backup import FakeBackup, mirror_lake
from tests.support.calendar import FakeCalendar, SessionTimes, et, weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import NTFY_TOPIC, PING_KEY, write_config
from tests.support.lake import FixtureLake
from tests.support.transport import FakeTransport

WEEK = date(2026, 8, 31)
DAY = date(2026, 8, 24)
SATURDAY = et(2026, 9, 5, 12, 0)
SUNDAY_20 = et(2026, 8, 30, 20, 0)
FRESH_MINT = et(2026, 8, 30, 19, 30)
REPEAT_ONLY = "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n"


def _url(slug: str) -> str:
    return f"https://hc-ping.com/{PING_KEY}/{slug}"


class Refusing:
    """A pinger healthchecks answers with a 404: the slug has no row.

    This is the failure the compaction job ran into on every run it ever made. It is a
    ``URLError`` subclass, so every call site's existing ``PING_FAILURES`` catch already
    swallowed it into a log line.
    """

    def __init__(self, slug: str = COMPACTION_SLUG, status: int = 404) -> None:
        self._slug = slug
        self._status = status

    def ping(self, url: str) -> None:
        raise urllib.error.HTTPError(url, self._status, "Not Found", {}, None)


class Unreachable:
    """A pinger the network never carried. No status comes back, so nothing pages."""

    def ping(self, url: str) -> None:
        raise urllib.error.URLError(OSError("connection refused"))


class Sink:
    """A publisher recording each page. The real one POSTs to ntfy."""

    def __init__(self) -> None:
        self.sent: list[Message] = []

    def publish(self, message, *, now):
        self.sent.append(message)
        return None


def _paged(sink: Sink) -> list[str]:
    """The slug each refused-ping page named, in order."""
    return [page.body.split(":")[0] for page in sink.sent if page.event == PING_REFUSED_EVENT]


def _refused_pages(transport: FakeTransport) -> list[Message]:
    return [m for m in transport.messages if m.event == PING_REFUSED_EVENT]


# -- 1. the five producers ----------------------------------------------------


def test_the_compaction_job_escalates_a_refused_ping(tmp_path):
    # The run that found this. Twenty-one seals, 9.8 million rows, and `pinged=False` on
    # one line among them.
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    sink = Sink()
    result = compact(
        lake_root,
        clock=ManualClock(et(2026, 8, 24, 16, 30)),
        calendar=FakeCalendar(
            {DAY: SessionTimes(open=et(2026, 8, 24, 9, 30), close=et(2026, 8, 24, 16, 0))}
        ),
        backup=FakeBackup(),
        backup_target=tmp_path / "ssd",
        pinger=Refusing(),
        ping_url=_url(COMPACTION_SLUG),
        publisher=sink,
        plan_path=tmp_path / "chain_plan.json",
    )
    # The run still reports everything it did. The page is what is new.
    assert result.pinged is False
    assert result.problem == "ping failed: HTTPError"
    assert _paged(sink) == [COMPACTION_SLUG]


def test_the_calendar_probe_escalates_a_refused_ping():
    sink = Sink()
    code = report(
        ProbeResult(date(2026, 9, 5), checked=True),
        publisher=sink,
        pinger=Refusing(CALENDAR_PROBE_SLUG),
        ping_url=_url(CALENDAR_PROBE_SLUG),
        slug=CALENDAR_PROBE_SLUG,
        now=et(2026, 9, 5, 9, 35),
    )
    # A quiet day still owes the page, because the ping is fed on every answer.
    assert code == 0
    assert _paged(sink) == [CALENDAR_PROBE_SLUG]


def test_the_pre_open_self_check_escalates_a_refused_ping():
    sink = Sink()
    outcome = cp.self_check(
        probe=lambda label: True,
        pinger=Refusing(PRE_OPEN_SLUG),
        ping_url=_url(PRE_OPEN_SLUG),
        # Saturday, so no assertion is owed and this stays about the ping.
        now=SATURDAY,
        publisher=sink,
    )
    assert outcome.pinged is False
    assert outcome.problem == "ping failed: HTTPError"
    assert _paged(sink) == [PRE_OPEN_SLUG]


def test_the_sunday_job_escalates_a_refused_ping(fixture_lake):
    lake_root = fixture_lake.with_chains("SPY", date(2026, 8, 28)).build()
    mirror_lake(lake_root, lake_root.parent / "ssd")
    sink = Sink()
    outcome = cp.sunday_maintenance(
        lake_root=lake_root,
        backup_target=lake_root.parent / "ssd",
        now=SUNDAY_20,
        calendar=weekday_sessions(date(2026, 8, 31), date(2026, 9, 7)),
        schedule_reader=lambda: REPEAT_ONLY,
        pinger=Refusing(SUNDAY_SLUG),
        ping_url=_url(SUNDAY_SLUG),
        mint=FRESH_MINT,
        canary=lambda: True,
        escalation=SlugEscalation(sink),
    )
    # A clean evening, so the ping was owed and the refusal is the only failure.
    assert outcome.pinged is False
    assert outcome.problems == ("ping failed: HTTPError",)
    assert _paged(sink) == [SUNDAY_SLUG]


def _deadman(at: datetime, pinger, sink: Sink | None = None) -> DeadMan:
    return DeadMan(
        pinger=pinger,
        url=_url(CAPTURE_SLUG),
        session_clock=SessionClock(clock=ManualClock(start=at), calendar=weekday_sessions(WEEK)),
        publisher=sink,
    )


def test_the_capture_dead_man_escalates_a_refused_ping():
    # The highest-value site. A slug that is wrong here never arms the check, so the
    # whole-daemon guarantee sits inert while every other job reports healthy.
    at = et(2026, 9, 2, 12, 0)
    sink = Sink()
    assert not _deadman(at, Refusing(CAPTURE_SLUG), sink).captured(at)
    assert _paged(sink) == [CAPTURE_SLUG]


def test_the_dead_man_still_swallows_its_failure_and_answers_the_panel():
    # Silence is the alarm and the return value feeds the Now panel, so neither the
    # page nor the raise may replace it.
    at = et(2026, 9, 2, 12, 0)
    recorded: list[datetime] = []
    deadman = DeadMan(
        pinger=Refusing(CAPTURE_SLUG),
        url=_url(CAPTURE_SLUG),
        session_clock=SessionClock(clock=ManualClock(start=at), calendar=weekday_sessions(WEEK)),
        recorder=recorded.append,
        publisher=Sink(),
    )
    assert deadman.captured(at) is False
    assert recorded == []


def test_the_dead_man_pages_once_and_re_arms_on_a_landed_ping():
    # It pings roughly 390 times a session inside a daemon that outlives every one-shot
    # job, so it is the one site that needs the state.
    at = et(2026, 9, 2, 12, 0)
    sink = Sink()

    class Flaky:
        def __init__(self) -> None:
            self.land = False

        def ping(self, url: str) -> None:
            if self.land:
                return
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)

    pinger = Flaky()
    deadman = _deadman(at, pinger, sink)
    assert not deadman.captured(at)
    assert not deadman.captured(at)
    assert _paged(sink) == [CAPTURE_SLUG]
    pinger.land = True
    assert deadman.captured(at)
    pinger.land = False
    assert not deadman.captured(at)
    assert _paged(sink) == [CAPTURE_SLUG, CAPTURE_SLUG]


@pytest.mark.parametrize(
    "drive",
    [
        pytest.param(
            lambda sink, tmp_path: compact(
                tmp_path / "lake",
                clock=ManualClock(et(2026, 8, 24, 16, 30)),
                calendar=FakeCalendar(
                    {DAY: SessionTimes(open=et(2026, 8, 24, 9, 30), close=et(2026, 8, 24, 16, 0))}
                ),
                backup=FakeBackup(),
                backup_target=tmp_path / "ssd",
                pinger=Unreachable(),
                ping_url=_url(COMPACTION_SLUG),
                publisher=sink,
                plan_path=tmp_path / "chain_plan.json",
            ),
            id="compaction",
        ),
        pytest.param(
            lambda sink, tmp_path: report(
                ProbeResult(date(2026, 9, 5), checked=True),
                publisher=sink,
                pinger=Unreachable(),
                ping_url=_url(CALENDAR_PROBE_SLUG),
                slug=CALENDAR_PROBE_SLUG,
                now=et(2026, 9, 5, 9, 35),
            ),
            id="calendar-probe",
        ),
        pytest.param(
            lambda sink, tmp_path: cp.self_check(
                probe=lambda label: True,
                pinger=Unreachable(),
                ping_url=_url(PRE_OPEN_SLUG),
                now=SATURDAY,
                publisher=sink,
            ),
            id="pre-open",
        ),
        pytest.param(
            lambda sink, tmp_path: _deadman(et(2026, 9, 2, 12, 0), Unreachable(), sink).captured(
                et(2026, 9, 2, 12, 0)
            ),
            id="capture",
        ),
    ],
)
def test_a_ping_lost_in_transport_pages_from_no_producer(drive, tmp_path):
    """A wifi blip drops pings while capture keeps journaling locally.

    The design already answers this one. The dead-man's grace is set looser than the
    watchdog's for exactly this, so healthchecks pages from outside if the outage lasts.
    A page raised from the laptop would fail in the same outage that dropped the ping.
    """
    (tmp_path / "lake").mkdir(exist_ok=True)
    sink = Sink()
    drive(sink, tmp_path)
    assert sink.sent == []


# -- 2. the five composition roots --------------------------------------------


def test_the_compaction_entry_pages_through_a_real_publisher(tmp_path, monkeypatch, capsys):
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    transport = FakeTransport()
    monkeypatch.setattr(compact_module, "UrllibPinger", Refusing)
    monkeypatch.setattr(compact_module, "NtfyTransport", lambda topic: transport)
    # Left live this would rsync a throwaway lake onto the machine running the suite.
    monkeypatch.setattr(compact_module, "RsyncBackup", FakeBackup)
    code = compact_module.main(
        ["--config", str(config), "--plan", str(tmp_path / "chain_plan.json")],
        clock=ManualClock(et(2026, 8, 24, 16, 30)),
        calendar=FakeCalendar(
            {DAY: SessionTimes(open=et(2026, 8, 24, 9, 30), close=et(2026, 8, 24, 16, 0))}
        ),
    )
    assert code == 0
    assert [m.body.split(":")[0] for m in _refused_pages(transport)] == [COMPACTION_SLUG]
    assert PING_KEY not in capsys.readouterr().out


def test_the_calendar_probe_entry_pages_through_a_real_publisher(tmp_path, monkeypatch):
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("SPY: {options: true, chain_cadence: 1m}\n")
    token = tmp_path / "token.json"
    token.write_text(json.dumps({"token": {"x": "never-read"}}))
    transport = FakeTransport()

    class _Vendor:
        @staticmethod
        def from_token(path, *, api_key, app_secret):
            return _Vendor()

        def get_quotes(self, symbols):
            return {}

    monkeypatch.setattr("lake.schwab.SchwabVendor", _Vendor)
    monkeypatch.setattr("lake.runner.UrllibPinger", Refusing)
    monkeypatch.setattr("lake.alert.NtfyTransport", lambda topic: transport)
    probe_module.main(["--config", str(config), "--tickers", str(tickers), "--token", str(token)])
    assert [m.body.split(":")[0] for m in _refused_pages(transport)] == [CALENDAR_PROBE_SLUG]


def test_the_self_check_entry_pages_through_a_real_publisher(tmp_path, monkeypatch, capsys):
    config = write_config(tmp_path, tmp_path / "lake")
    (tmp_path / "lake").mkdir(exist_ok=True)
    transport = FakeTransport()
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
    monkeypatch.setattr(cp, "UrllibPinger", Refusing)
    monkeypatch.setattr(cp, "NtfyTransport", lambda topic: transport)
    # Saturday, so nothing is owed and the ping is what this measures.
    monkeypatch.setattr(cp, "_system_clock", lambda: ManualClock(start=SATURDAY))
    code = cp.main(["self-check", "--config", str(config)])
    assert code == 1
    assert [m.body.split(":")[0] for m in _refused_pages(transport)] == [PRE_OPEN_SLUG]
    assert PING_KEY not in capsys.readouterr().out


def _excluded(paths: Sequence[str]) -> str:
    """A reader reporting every path already excluded, the healthy Time Machine state."""
    return "".join(f"[Excluded]\t{p}\n" for p in paths)


def test_the_sunday_entry_pages_through_a_real_publisher(tmp_path, monkeypatch, capsys):
    lake_root = FixtureLake(tmp_path / "lake").with_chains("SPY", date(2026, 8, 28)).build()
    config = write_config(tmp_path, lake_root)
    mirror_lake(lake_root, tmp_path / "ssd")
    token = tmp_path / "token.json"
    token.write_text(
        json.dumps({"creation_timestamp": FRESH_MINT.timestamp(), "token": {"x": "never-read"}})
    )
    transport = FakeTransport()
    monkeypatch.setattr(cp, "read_pmset_schedule", lambda: REPEAT_ONLY)
    monkeypatch.setattr(cp, "UrllibPinger", Refusing)
    monkeypatch.setattr(cp, "token_canary", lambda **kwargs: lambda: True)
    monkeypatch.setattr(cp, "NtfyTransport", lambda topic: transport)
    monkeypatch.setattr(cp, "read_exclusions", _excluded)
    code = cp.main(
        ["sunday", "--config", str(config), "--token", str(token)],
        clock=ManualClock(start=SUNDAY_20),
        calendar=weekday_sessions(date(2026, 8, 31), date(2026, 9, 7)),
    )
    assert code == 1
    assert [m.body.split(":")[0] for m in _refused_pages(transport)] == [SUNDAY_SLUG]
    printed = capsys.readouterr().out
    # The Sunday job is the one that is not one-shot. A refused ping is a problem, so
    # the retry loop re-runs the whole job every half hour until the canary deadline.
    # Seven attempts, one page.
    assert "attempts=7" in printed
    assert printed.count("ping failed: HTTPError") == 7
    assert PING_KEY not in printed


def test_no_page_a_root_sent_carried_a_secret(tmp_path, monkeypatch, capsys):
    """Each root builds its publisher holding the two values that must never reach a phone.

    A page carrying either is refused rather than sent, so a page arriving at the
    transport is itself the proof. This names the secrets so the assertion reads as one.
    """
    config = write_config(tmp_path, tmp_path / "lake")
    (tmp_path / "lake").mkdir(exist_ok=True)
    transport = FakeTransport()
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
    monkeypatch.setattr(cp, "UrllibPinger", Refusing)
    monkeypatch.setattr(cp, "NtfyTransport", lambda topic: transport)
    monkeypatch.setattr(cp, "_system_clock", lambda: ManualClock(start=SATURDAY))
    cp.main(["self-check", "--config", str(config)])
    page = _refused_pages(transport)[0]
    assert PING_KEY not in page.body and NTFY_TOPIC not in page.body
    assert PING_KEY not in page.title and NTFY_TOPIC not in page.title
    capsys.readouterr()
