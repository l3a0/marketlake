"""The page publisher: the cap, the refusal, and the record of what never sent."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from lake import alert
from lake.alert import (
    CAP_REACHED,
    PAGE_PRIORITY,
    POST_FAILED,
    REFUSED,
    Message,
    Publisher,
    undelivered,
)
from tests.support.clock import ManualClock
from tests.support.config import NTFY_TOPIC, PING_KEY, write_config

ET = ZoneInfo("America/New_York")
NOW = datetime(2026, 9, 2, 10, 0, tzinfo=ET)
PAGE = Message(event="capture_down", title="Capture down: SPY chains", body="3 minutes")
# What must never reach a phone: the ping key and the ntfy topic.
SECRETS = ("SECRETKEY", "secret-topic")


class Recording:
    def __init__(self) -> None:
        self.sent: list[Message] = []

    def send(self, message: Message) -> None:
        self.sent.append(message)


class Broken:
    def send(self, message: Message) -> None:
        raise OSError("network down")


def _records(root, day: date = date(2026, 9, 2)) -> list[dict]:
    directory = root / "reports" / "alerts" / f"date={day.isoformat()}"
    if not directory.is_dir():
        return []
    return [json.loads(p.read_text()) for p in sorted(directory.glob("*.json"))]


def test_a_page_that_sends_leaves_no_record(tmp_path):
    transport = Recording()
    publisher = Publisher(lake_root=tmp_path, transport=transport, pid=1)
    assert publisher.publish(PAGE, now=NOW).sent
    assert transport.sent == [PAGE]
    assert _records(tmp_path) == []


def test_a_failed_post_never_raises_and_is_written_down(tmp_path):
    # A watchdog that crashes while reporting a dead surface is worse than a quiet one.
    publisher = Publisher(lake_root=tmp_path, transport=Broken(), pid=1)
    delivery = publisher.publish(PAGE, now=NOW)
    assert not delivery.sent
    assert delivery.reason == POST_FAILED
    (record,) = _records(tmp_path)
    assert record["event"] == "capture_down"
    assert record["reason"] == POST_FAILED
    assert record["detail"] == "OSError"


def test_the_cap_stops_sending_and_still_records(tmp_path):
    transport = Recording()
    publisher = Publisher(lake_root=tmp_path, transport=transport, daily_cap=2, pid=1)
    for minute in range(4):
        publisher.publish(PAGE, now=NOW.replace(minute=minute))
    assert len(transport.sent) == 2
    reasons = [r["reason"] for r in _records(tmp_path)]
    assert reasons == [CAP_REACHED, CAP_REACHED]


def test_the_cap_resets_when_the_session_date_turns(tmp_path):
    transport = Recording()
    publisher = Publisher(lake_root=tmp_path, transport=transport, daily_cap=1, pid=1)
    publisher.publish(PAGE, now=NOW)
    publisher.publish(PAGE, now=NOW)
    assert len(transport.sent) == 1
    publisher.publish(PAGE, now=NOW.replace(day=3))
    assert len(transport.sent) == 2


@pytest.mark.parametrize(
    "message",
    [
        Message(event="e", title="see https://hc-ping.com/SECRETKEY/capture", body="b"),
        Message(event="e", title="t", body="posted to https://ntfy.sh/secret-topic"),
        # The key on its own, with no URL around it, is the same leak.
        Message(event="e", title="t", body="key SECRETKEY rotated"),
    ],
)
def test_a_page_carrying_a_secret_is_refused_and_the_record_redacts_it(tmp_path, message):
    transport = Recording()
    publisher = Publisher(lake_root=tmp_path, transport=transport, secrets=SECRETS, pid=1)
    delivery = publisher.publish(message, now=NOW)

    assert not delivery.sent
    assert delivery.reason == REFUSED
    assert transport.sent == []
    (record,) = _records(tmp_path)
    # The record must not repeat what the refusal objected to.
    assert "title" not in record
    text = json.dumps(record)
    assert "SECRETKEY" not in text
    assert "secret-topic" not in text


def test_a_record_never_carries_the_body(tmp_path):
    # A body carries whatever a producer put in it, and this file sits inside the
    # directories the dashboard may read.
    publisher = Publisher(lake_root=tmp_path, transport=Broken(), pid=1)
    publisher.publish(Message(event="e", title="t", body="a very private thing"), now=NOW)
    assert "a very private thing" not in json.dumps(_records(tmp_path))


def test_the_count_of_undelivered_pages_is_zero_on_an_ordinary_day(tmp_path):
    # The ordinary day has none, and a SQL read over an empty glob raises rather than
    # returning zero, so this is counted from the filesystem.
    assert undelivered(tmp_path, date(2026, 9, 2)) == 0


def test_the_count_sees_what_was_written(tmp_path):
    publisher = Publisher(lake_root=tmp_path, transport=Broken(), pid=1)
    for minute in range(3):
        publisher.publish(PAGE, now=NOW.replace(minute=minute))
    assert undelivered(tmp_path, date(2026, 9, 2)) == 3
    assert undelivered(tmp_path, date(2026, 9, 3)) == 0


def test_a_publisher_with_no_transport_records_rather_than_raising(tmp_path):
    publisher = Publisher(lake_root=tmp_path, pid=1)
    delivery = publisher.publish(PAGE, now=NOW)
    assert not delivery.sent
    assert delivery.reason == POST_FAILED


def test_a_public_hostname_alone_is_not_a_leak(tmp_path):
    # `hc-ping.com` and `ntfy.sh` are public names. Refusing a page for mentioning one
    # would drop a real alert and protect nothing.
    transport = Recording()
    publisher = Publisher(lake_root=tmp_path, transport=transport, secrets=SECRETS, pid=1)
    message = Message(event="e", title="hc-ping.com unreachable", body="ntfy.sh down too")
    assert publisher.publish(message, now=NOW).sent
    assert transport.sent == [message]


def test_a_publisher_holding_no_secrets_refuses_nothing(tmp_path):
    transport = Recording()
    publisher = Publisher(lake_root=tmp_path, transport=transport, pid=1)
    assert publisher.publish(PAGE, now=NOW).sent


# -- the production wiring -----------------------------------------------------------


def test_the_daemon_pages_through_the_publisher_when_a_surface_goes_quiet(tmp_path):
    """Deleting the daemon's watchdog binding must not leave the suite green."""
    from lake import daemon
    from lake.capture import CycleResult, SegmentOutcome
    from tests.support.calendar import et, weekday_sessions
    from tests.support.pinger import FakePinger

    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("XYZ: {options: false}\n")

    def failing_cycle(*, close_tag, session_phase):
        return CycleResult(
            et(2026, 9, 2, 12, 0),
            (
                SegmentOutcome(
                    surface="quotes",
                    ticker="XYZ",
                    path=tmp_path / "s.arrows",
                    partition="p",
                    row_kind="gap",
                    rows=1,
                    error_class="boom",
                    fetched_at=None,
                ),
            ),
        )

    clock = ManualClock(start=et(2026, 9, 2, 11, 58))
    ticks = [0]

    def four() -> bool:
        ticks[0] += 1
        return ticks[0] <= 4

    daemon.run_loop_from_config(
        config_path=str(config),
        tickers_path=str(tickers),
        clock=clock,
        calendar=weekday_sessions(date(2026, 8, 31)),
        assertion_runner=lambda args: None,
        transport=Broken(),
        pinger=FakePinger(),
        compaction_runner=lambda args: None,
        cycle_runner=failing_cycle,
        should_continue=four,
    )

    # The ntfy POST cannot land in a test, so the page is recorded as undelivered. That
    # it was recorded at all is the proof the watchdog reached the publisher.
    records = _records(lake_root)
    assert records, "the daemon ran four failing cycles and raised no page"
    assert records[0]["event"] == "capture_down"


def test_every_page_of_one_burst_is_written_down(tmp_path):
    """One cycle raises several pages at one instant.

    A slot carries no sub-minute part, so a name built from the clock alone collides and
    all but the first is lost to a swallowed FileExistsError.
    """
    publisher = Publisher(lake_root=tmp_path, transport=Broken(), pid=1)
    titles = ["Capture down: quote sampler dead", "Capture down: SPY chains", "x"]
    for title in titles:
        delivery = publisher.publish(Message(event="capture_down", title=title, body="b"), now=NOW)
        assert delivery.recorded
    assert undelivered(tmp_path, date(2026, 9, 2)) == 3
    assert sorted(r["title"] for r in _records(tmp_path)) == sorted(titles)


def test_a_record_that_cannot_be_written_says_so(tmp_path, capsys, monkeypatch):
    publisher = Publisher(lake_root=tmp_path, transport=Broken(), pid=1)

    def refuse(*args, **kwargs):
        raise OSError("read-only")

    monkeypatch.setattr("builtins.open", refuse)
    delivery = publisher.publish(PAGE, now=NOW)
    assert not delivery.sent
    assert not delivery.recorded
    # Lost twice is not the same as lost once, and the log is the only place left.
    assert "record failed too" in capsys.readouterr().err


def test_an_undelivered_page_never_creates_the_lake_root(tmp_path, capsys):
    """A publisher that conjured the lake would turn a broken install into a green check.

    The Sunday job decides whether to ping on ``root.is_dir()`` and re-reads that on every
    retry. Before this, a failed push recorded the page through ``mkdir(parents=True)``,
    which created the lake root itself, so attempt two found a lake, scrubbed an empty
    directory, found no problems, and pinged the `sunday` check green on a lake that did
    not exist. The record is written inside a lake that exists, or not at all.
    """
    missing = tmp_path / "not-a-lake"
    publisher = Publisher(lake_root=missing, transport=Broken(), pid=1)

    delivery = publisher.publish(PAGE, now=NOW)

    assert not delivery.sent
    assert not delivery.recorded
    assert not missing.exists(), "the publisher created the lake root it was handed"
    # Lost twice, so the log is the only place left to say so.
    assert "record failed too" in capsys.readouterr().err


def test_the_topic_never_appears_in_the_request_url(tmp_path):
    # A URL lands in a proxy log and anything that records a request line. The topic is
    # the write credential for the channel.
    from lake.alert import PAGE_TAG, NtfyTransport

    sent = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def urlopen(request, timeout=None):
        sent["url"] = request.full_url
        sent["body"] = json.loads(request.data)
        return FakeResponse()

    import urllib.request

    original = urllib.request.urlopen
    urllib.request.urlopen = urlopen
    try:
        NtfyTransport("secret-topic").send(PAGE)
    finally:
        urllib.request.urlopen = original

    assert "secret-topic" not in sent["url"]
    assert sent["url"] == "https://ntfy.sh"
    assert sent["body"]["topic"] == "secret-topic"
    assert sent["body"]["title"] == PAGE.title
    assert sent["body"]["tags"] == [PAGE_TAG]


def test_only_a_page_carries_the_tag():
    # The design gives the emoji one job: marking a message from the lake's own jobs as
    # a page. A reminder and the nightly summary carry none, so the phone can tell the
    # tiers apart at a glance. Priority already names the tier, so the tag follows it.
    from lake.alert import PAGE_TAG, NtfyTransport

    bodies = []

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def urlopen(request, timeout=None):
        bodies.append(json.loads(request.data))
        return FakeResponse()

    import urllib.request

    original = urllib.request.urlopen
    urllib.request.urlopen = urlopen
    try:
        for priority in (5, 3, 2):
            NtfyTransport("secret-topic").send(
                Message(event="sunday_reauth", title="t", body="b", priority=priority)
            )
    finally:
        urllib.request.urlopen = original

    assert bodies[0]["tags"] == [PAGE_TAG]
    assert "tags" not in bodies[1]
    assert "tags" not in bodies[2]


# -- the hand-run channel test -------------------------------------------------------
#
# ``python -m lake.alert --test-push`` proves the one assumption every page rides: that a
# priority-5 push reaches the phone and interrupts it. These drive `main` with the real
# wiring and a fake at the leaf, the way the other command entries are tested, because a
# shortcut around `main` would leave the wiring this command exists to exercise untested.


def _no_secret(captured) -> None:
    """Neither stream carries the ntfy topic or the ping key.

    One sweep, called from every outcome below, so a new outcome cannot get a weaker
    check than the others. The topic is the write credential for the channel and this
    command's whole job is to report on a push that carried it, so an operator pasting
    the output into an issue is exactly the shape of leak to expect here.
    """
    for stream in (captured.out, captured.err):
        assert NTFY_TOPIC not in stream
        assert PING_KEY not in stream


def _config(tmp_path, *, lake_exists: bool = True) -> tuple[Path, Path]:
    """A config naming a lake under ``tmp_path``, and that lake's root.

    ``lake_exists=False`` leaves the root uncreated, which is what makes the publisher
    unable to write its record. That is the third outcome, the one lost twice.
    """
    lake_root = tmp_path / "lake"
    if lake_exists:
        lake_root.mkdir()
    return write_config(tmp_path, lake_root), lake_root


def _push(tmp_path, monkeypatch, transport, *, lake_exists: bool = True) -> tuple[int, Path]:
    """Run the entry against ``transport``, and return its exit code and the lake root."""
    config, lake_root = _config(tmp_path, lake_exists=lake_exists)
    monkeypatch.setattr(alert, "NtfyTransport", lambda topic: transport)
    code = alert.main(["--test-push", "--config", str(config)], clock=ManualClock(start=NOW))
    return code, lake_root


def test_a_test_push_that_sends_exits_zero_and_says_what_went_out(tmp_path, capsys, monkeypatch):
    transport = Recording()
    code, lake_root = _push(tmp_path, monkeypatch, transport)

    assert code == 0
    (sent,) = transport.sent
    assert sent.event == alert.TEST_PUSH_EVENT
    assert sent.title == alert.TEST_PUSH_TITLE
    captured = capsys.readouterr()
    assert "sent at priority 5" in captured.out
    # A success sends, so there is nothing to write down.
    assert _records(lake_root) == []
    _no_secret(captured)


def test_a_refused_test_push_exits_non_zero_and_prints_the_reason(tmp_path, capsys, monkeypatch):
    """Recorded is not sent.

    A page written down because it could not be sent left the phone silent. Exiting 0
    here would hand an operator a green result for a channel that does not work, which
    is the one failure this command exists to catch.
    """
    code, lake_root = _push(tmp_path, monkeypatch, Broken())

    assert code != 0
    captured = capsys.readouterr()
    assert POST_FAILED in captured.out
    assert "NOT sent" in captured.out
    # The record is there. The exit code still says the channel did not carry it.
    (record,) = _records(lake_root)
    assert record["reason"] == POST_FAILED
    _no_secret(captured)


def test_a_test_push_lost_twice_says_it_was_lost_twice(tmp_path, capsys, monkeypatch):
    """Not sent and not written down is a third outcome, not a louder second one.

    An operator whose lake root is missing has a push that reached nothing and a record
    that reached nothing either, so there is no file to go back to.
    """
    code, lake_root = _push(tmp_path, monkeypatch, Broken(), lake_exists=False)

    assert code != 0
    captured = capsys.readouterr()
    assert "lost twice" in captured.out
    assert not lake_root.exists(), "the test push created the lake root it was handed"
    _no_secret(captured)


def test_the_test_push_goes_at_the_page_tier(tmp_path, capsys, monkeypatch):
    """Priority 5 is the half that matters.

    The design says this push is the evidence that a page interrupts a locked iPhone. At
    the report tier it would prove delivery and prove nothing about the interrupt, and
    the interrupt is what every alarm depends on. The literal is pinned alongside the
    constant, so moving the constant cannot quietly move the test.
    """
    transport = Recording()
    code, _ = _push(tmp_path, monkeypatch, transport)

    assert code == 0
    (sent,) = transport.sent
    assert sent.priority == PAGE_PRIORITY == 5
    _no_secret(capsys.readouterr())


def test_the_test_push_goes_through_the_publisher(tmp_path, capsys, monkeypatch):
    """A direct POST would reach the phone and prove a path production does not use.

    The publisher is where the daily cap, the secret refusal, and the record of an
    undelivered page live. A send that skipped it would exercise none of them, so a
    green result would say nothing about what happens when the daemon pages.
    """
    seen: list[Message] = []
    built: dict = {}
    real = alert.Publisher

    class Watched(real):
        def __init__(self, **kwargs):
            built.update(kwargs)
            super().__init__(**kwargs)

        def publish(self, message: Message, *, now):
            seen.append(message)
            return super().publish(message, now=now)

    monkeypatch.setattr(alert, "Publisher", Watched)
    code, lake_root = _push(tmp_path, monkeypatch, Broken())

    assert code != 0
    assert [message.event for message in seen] == [alert.TEST_PUSH_EVENT]
    # The publisher is armed the same way every other producer arms it, so the test push
    # is refused if it ever carries a secret.
    assert built["secrets"] == (PING_KEY, NTFY_TOPIC)
    assert built["lake_root"] == lake_root
    # The record is the publisher's own mark on the filesystem. A direct POST leaves none.
    assert undelivered(lake_root, date(2026, 9, 2)) == 1
    _no_secret(capsys.readouterr())


def test_the_entry_refuses_a_bare_invocation_rather_than_paging(tmp_path, monkeypatch):
    """Running the module with no arguments must not send a page to a phone.

    ``--test-push`` names the one thing this entry does, and argparse refusing it is what
    keeps a stray ``python -m lake.alert`` from putting a priority-5 push on the topic.
    """
    monkeypatch.setattr(alert, "NtfyTransport", lambda topic: Recording())
    with pytest.raises(SystemExit) as excinfo:
        alert.main([])
    assert excinfo.value.code == 2
