"""The page publisher: the cap, the refusal, and the record of what never sent."""

from __future__ import annotations

import json
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from lake.alert import (
    CAP_REACHED,
    POST_FAILED,
    REFUSED,
    Message,
    Publisher,
    undelivered,
)

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
    from tests.support.clock import ManualClock
    from tests.support.config import write_config
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
