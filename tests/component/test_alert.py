"""The page publisher: the cap, the refusal, and the record of what never sent."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import NamedTuple
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
from tests.support.config import (
    NTFY_TOPIC,
    PING_KEY,
    SCHWAB_API_KEY,
    SCHWAB_APP_SECRET,
    write_config,
)
from tests.support.schema_version import record_running_version

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
    # The running version recorded, so the startup check marketlake #130 added stays quiet and
    # the first page this run raises is the watchdog's, which is what the case is about.
    record_running_version(lake_root)
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
# ``python -m lake.alert --test-push`` exercises the one assumption every page rides: that
# a priority-5 push reaches the phone and interrupts it. These drive `main` with the real
# wiring and a fake at the leaf, the way the other command entries are tested, because a
# shortcut around `main` would leave the wiring this command exists to exercise untested.

UTC_NOON = datetime(2026, 9, 2, 16, 0, tzinfo=UTC)
"""16:00 UTC is 12:00 ET on this date.

Deliberately not Eastern. `Clock.now()` returns UTC by contract, so a fixture already in
Eastern hides a missing conversion: the body would read the same either way.
"""


def _no_secret(captured) -> None:
    """Neither stream carries any of the four secrets `config.yaml` holds.

    One sweep, called from every outcome below, so a new outcome cannot get a weaker
    check than the others. `main` loads the whole config, so all four are live in the
    frame, and the design puts the Schwab pair in the same class as the ping key and the
    topic. A sweep for two of four passes a command that prints the other two.
    """
    for stream in (captured.out, captured.err):
        for secret in (NTFY_TOPIC, PING_KEY, SCHWAB_API_KEY, SCHWAB_APP_SECRET):
            assert secret not in stream


def _config(tmp_path, *, lake_exists: bool = True) -> tuple[Path, Path]:
    """A config naming a lake under ``tmp_path``, and that lake's root.

    ``lake_exists=False`` leaves the root uncreated, which is what makes the publisher
    unable to write its record. That is the third outcome, the one lost twice.
    """
    lake_root = tmp_path / "lake"
    if lake_exists:
        lake_root.mkdir()
    return write_config(tmp_path, lake_root), lake_root


class _Run(NamedTuple):
    """What one drive of the entry produced."""

    code: int
    lake_root: Path
    topics: list[str]
    built: dict


def _push(tmp_path, monkeypatch, transport, *, lake_exists=True, config=None, now=UTC_NOON):
    """Drive `main` against ``transport``, capturing what the entry built on the way.

    The topic reaching `NtfyTransport` and the keywords reaching `Publisher` are captured
    rather than discarded. A patch that throws them away cannot tell the configured topic
    from any other string, and substituting the ping key there would publish it to ntfy.
    """
    if config is None:
        config, lake_root = _config(tmp_path, lake_exists=lake_exists)
    else:
        lake_root = Path(yaml_value(config, "lake_root"))
    topics: list[str] = []
    built: dict = {}
    real = alert.Publisher

    class Watched(real):
        def __init__(self, **kwargs):
            built.update(kwargs)
            super().__init__(**kwargs)

    def factory(topic):
        topics.append(topic)
        return transport

    monkeypatch.setattr(alert, "NtfyTransport", factory)
    monkeypatch.setattr(alert, "Publisher", Watched)
    code = alert.main(["--test-push", "--config", str(config)], clock=ManualClock(start=now))
    return _Run(code, lake_root, topics, built)


def yaml_value(path: Path, key: str) -> str:
    """One scalar out of a hand-written config, without importing the loader."""
    for line in Path(path).read_text().splitlines():
        if line.startswith(f"{key}:"):
            return line.split(":", 1)[1].strip()
    raise KeyError(key)


def test_a_test_push_that_sends_exits_zero_and_says_what_went_out(tmp_path, capsys, monkeypatch):
    transport = Recording()
    run = _push(tmp_path, monkeypatch, transport)

    assert run.code == 0
    (sent,) = transport.sent
    assert sent.event == alert.TEST_PUSH_EVENT
    assert sent.title == alert.TEST_PUSH_TITLE
    captured = capsys.readouterr()
    assert "accepted the push at priority 5" in captured.out
    # Acceptance is not delivery, and saying so is the point. An unauthenticated topic
    # accepts a mistyped name, so a zero exit alone would be read as proof it works.
    assert "read by nobody" in captured.out
    assert "the phone is the only evidence" in captured.out
    # A success sends, so there is nothing to write down.
    assert _records(run.lake_root) == []
    _no_secret(captured)


def test_the_message_carries_the_pinned_event_name_and_title(tmp_path, capsys, monkeypatch):
    """The literals, not the constants.

    Asserting `sent.event == alert.TEST_PUSH_EVENT` moves with the constant and so holds
    nothing. An operator reads the topic against a fixed shape, so the shape is the thing
    that must not drift.
    """
    assert alert.TEST_PUSH_EVENT == "test_push"
    assert alert.TEST_PUSH_TITLE == "Test push"
    transport = Recording()
    _push(tmp_path, monkeypatch, transport)
    (sent,) = transport.sent
    assert sent.event == "test_push"
    assert sent.title == "Test push"
    _no_secret(capsys.readouterr())


def test_the_body_stamps_eastern_and_says_nothing_is_wrong(tmp_path, capsys, monkeypatch):
    """`Clock.now()` is UTC, so the conversion to Eastern is a real step.

    The body prints the letters ET, so a missing or wrong conversion still reads as an
    Eastern time and is invisible to the eye. The clock here is 16:00 UTC, which is 12:00
    in New York, and no other zone gives that answer.
    """
    transport = Recording()
    _push(tmp_path, monkeypatch, transport)

    (sent,) = transport.sent
    assert "2026-09-02 12:00:00 ET" in sent.body
    # It arrives at the page tier with a page's emoji, possibly at 3am. A body that read
    # like a real page would teach the operator to distrust the tier.
    assert "Nothing is wrong" in sent.body
    _no_secret(capsys.readouterr())


def test_the_topic_comes_from_the_config_every_other_producer_reads(tmp_path, capsys, monkeypatch):
    """Substituting the ping key here would publish it to ntfy.

    `NtfyTransport` puts the topic in the POST body, and `Publisher._leak` scans only the
    message title and body, never the transport. So the secret refusal cannot catch this
    one, and a patch that discards the topic cannot tell the two apart.
    """
    run = _push(tmp_path, monkeypatch, Recording())

    assert run.topics == [NTFY_TOPIC]
    _no_secret(capsys.readouterr())


def test_a_refused_test_push_exits_non_zero_and_prints_the_reason(tmp_path, capsys, monkeypatch):
    """Recorded is not sent.

    A page written down because it could not be sent left the phone silent. Exiting 0
    here would hand an operator a green result for a channel that does not work, which
    is the one failure this command exists to catch.
    """
    run = _push(tmp_path, monkeypatch, Broken())

    assert run.code != 0
    captured = capsys.readouterr()
    assert POST_FAILED in captured.err
    assert "NOT sent" in captured.err
    # Written down and lost twice are different outcomes. Telling an operator whose page
    # was recorded that nothing records it sends them past the file that holds it.
    assert "written down under reports/alerts/" in captured.err
    assert "lost twice" not in captured.err
    # The record is there. The exit code still says the channel did not carry it.
    (record,) = _records(run.lake_root)
    assert record["reason"] == POST_FAILED
    _no_secret(captured)


def test_a_test_push_lost_twice_says_it_was_lost_twice(tmp_path, capsys, monkeypatch):
    """Not sent and not written down is a third outcome, not a louder second one.

    An operator whose lake root is missing has a push that reached nothing and a record
    that reached nothing either, so there is no file to go back to.
    """
    run = _push(tmp_path, monkeypatch, Broken(), lake_exists=False)

    assert run.code != 0
    captured = capsys.readouterr()
    assert "lost twice" in captured.err
    assert "written down under reports/alerts/" not in captured.err
    assert not run.lake_root.exists(), "the test push created the lake root it was handed"
    _no_secret(captured)


def test_a_refusal_blames_the_config_rather_than_the_phone(tmp_path, capsys, monkeypatch):
    """The refusal is the one failure whose cause lives in `config.yaml`.

    `Publisher._leak` is a plain substring match of the topic against the message text,
    and this body is fixed English prose. A topic that is an ordinary word refuses every
    run identically, and the channel is never contacted. Sending the operator to check
    the phone would be an answer no phone-side fix can ever clear.
    """
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = tmp_path / "collide.yaml"
    config.write_text(
        f"lake_root: {lake_root}\n"
        f"backup_target: {tmp_path}\n"
        f"healthchecks_ping_key: {PING_KEY}\n"
        # A word the fixed body already uses, which is what makes the leak check fire.
        "ntfy_topic: interrupts\n"
        f"schwab_api_key: {SCHWAB_API_KEY}\n"
        f"schwab_app_secret: {SCHWAB_APP_SECRET}\n"
    )
    transport = Recording()
    run = _push(tmp_path, monkeypatch, transport, config=config)

    assert run.code != 0
    # The channel was never contacted, so nothing about the phone is in question.
    assert transport.sent == []
    captured = capsys.readouterr()
    assert REFUSED in captured.err
    assert "carries a value from config.yaml" in captured.err
    assert "the phone is not in question" in captured.err
    _no_secret(captured)


def test_the_test_push_goes_at_the_page_tier(tmp_path, capsys, monkeypatch):
    """Priority 5 is the half that matters.

    The design says this push is the evidence that a page interrupts a locked iPhone. At
    the report tier it would prove delivery and prove nothing about the interrupt, and
    the interrupt is what every alarm depends on. The literal sits beside the constant,
    so moving the constant cannot quietly move this test with it.
    """
    transport = Recording()
    run = _push(tmp_path, monkeypatch, transport)

    assert run.code == 0
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
    real = alert.Publisher

    class Watched(real):
        def publish(self, message: Message, *, now):
            seen.append(message)
            return super().publish(message, now=now)

    monkeypatch.setattr(alert, "Publisher", Watched)
    run = _push(tmp_path, monkeypatch, Broken())

    assert run.code != 0
    assert [message.event for message in seen] == [alert.TEST_PUSH_EVENT]
    # The publisher is armed the same way every other producer arms it, so the test push
    # is refused if it ever carries a secret.
    assert run.built["secrets"] == (PING_KEY, NTFY_TOPIC)
    assert run.built["lake_root"] == run.lake_root
    # No override, so the design's forty stands. A raised cap here would exempt the hand
    # run from the path it exists to exercise.
    assert "daily_cap" not in run.built
    assert alert.DEFAULT_DAILY_CAP == 40
    # The record is the publisher's own mark on the filesystem. A direct POST leaves none.
    assert undelivered(run.lake_root, date(2026, 9, 2)) == 1
    _no_secret(capsys.readouterr())


def test_the_entry_refuses_a_bare_invocation_rather_than_paging(tmp_path, capsys, monkeypatch):
    """Running the module with no arguments must not send a page to a phone.

    The config is valid and named, so argparse is the only thing left that can exit 2.
    Without that, a missing config exits 2 by its own route and the test passes whether
    or not the flag is required, which is the mutation it exists to catch.
    """
    config, _ = _config(tmp_path)
    monkeypatch.setenv("MARKETLAKE_CONFIG", str(config))
    built: list[str] = []
    monkeypatch.setattr(alert, "NtfyTransport", lambda topic: built.append(topic) or Recording())

    with pytest.raises(SystemExit) as excinfo:
        alert.main([])

    assert excinfo.value.code == 2
    assert "--test-push" in capsys.readouterr().err
    assert built == [], "a bare invocation built a transport and reached the topic"


def test_an_abbreviated_flag_does_not_reach_a_phone(tmp_path, capsys, monkeypatch):
    """argparse accepts any unambiguous prefix unless told not to.

    `--test` is what someone reaches for when they mean a dry run, and by default it
    parses as `--test-push` and puts a priority-5 push on the topic.
    """
    config, _ = _config(tmp_path)
    built: list[str] = []
    monkeypatch.setattr(alert, "NtfyTransport", lambda topic: built.append(topic) or Recording())

    with pytest.raises(SystemExit) as excinfo:
        alert.main(["--test", "--config", str(config)])

    assert excinfo.value.code == 2
    assert built == [], "an abbreviated flag built a transport and reached the topic"
