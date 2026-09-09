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


def test_a_roster_saved_mid_keystroke_leaves_the_daemon_running(tmp_path):
    """The roster fallback in ``on_skipped`` must cover a parse error, not only a bad shape.

    The hook re-reads the roster and catches ``TickersError``, so the last good one
    stays in place rather than the daemon refusing to count. A hand edit caught
    mid-save is the likeliest way the file goes bad, and it is a parse error. While the
    loader let ``yaml.YAMLError`` out, that walked past the hook, out of ``run_loop``,
    and took the process with it.
    """
    from lake import daemon
    from lake.capture import CycleResult
    from tests.support.calendar import et, weekday_sessions
    from tests.support.clock import ManualClock
    from tests.support.config import write_config
    from tests.support.pinger import FakePinger

    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("XYZ: {options: false}\n")

    clock = ManualClock(start=et(2026, 9, 2, 11, 58))

    def overrunning_cycle(*, close_tag, session_phase):
        # Three minutes for a one-minute cycle. The next tick lands past them, so the
        # loop hands the minutes it slept through to on_skipped, which is the one hook
        # that re-reads the roster. Halfway through, the operator's editor saves.
        clock.advance(180)
        tickers.write_text("XYZ: {options: fal")
        return CycleResult(et(2026, 9, 2, 12, 0), ())

    ticks = [0]

    def three() -> bool:
        ticks[0] += 1
        return ticks[0] <= 3

    daemon.run_loop_from_config(
        config_path=str(config),
        tickers_path=str(tickers),
        clock=clock,
        calendar=weekday_sessions(date(2026, 8, 31)),
        assertion_runner=lambda args: None,
        transport=Broken(),
        pinger=FakePinger(),
        cycle_runner=overrunning_cycle,
        should_continue=three,
    )

    # Four calls means should_continue stopped the loop, rather than an exception doing
    # it. Three of them ran a tick and the fourth said stop.
    assert ticks[0] == 4
    # The hook must also still charge the roster it kept, so three slept minutes trip
    # XYZ's counter and raise its page. An empty roster would survive just as quietly
    # and page nothing, which is the failure this guards.
    records = _records(lake_root)
    assert records, "the daemon slept through three capture minutes and raised no page"
    assert "XYZ" in records[0]["title"]


def test_a_ticker_retired_mid_session_stops_being_charged(tmp_path):
    """The other half of the ``on_skipped`` promise: the hook re-reads, it does not close over.

    The comment on the hook gives two rules. A roster that will not load keeps the last
    good one, and a ticker retired mid-session stops being charged without a restart.
    A refactor that closed over the startup roster would keep every other test green
    and go on charging a ticker the operator retired.
    """
    from lake import daemon
    from lake.capture import CycleResult
    from tests.support.calendar import et, weekday_sessions
    from tests.support.clock import ManualClock
    from tests.support.config import write_config
    from tests.support.pinger import FakePinger

    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("XYZ: {options: false}\nZZZ: {options: false}\n")

    clock = ManualClock(start=et(2026, 9, 2, 11, 58))

    def overrunning_cycle(*, close_tag, session_phase):
        clock.advance(180)
        # The operator retires ZZZ. The roster still loads, so no fallback is involved.
        tickers.write_text("XYZ: {options: false}\n")
        return CycleResult(et(2026, 9, 2, 12, 0), ())

    ticks = [0]

    def three() -> bool:
        ticks[0] += 1
        return ticks[0] <= 3

    daemon.run_loop_from_config(
        config_path=str(config),
        tickers_path=str(tickers),
        clock=clock,
        calendar=weekday_sessions(date(2026, 8, 31)),
        assertion_runner=lambda args: None,
        transport=Broken(),
        pinger=FakePinger(),
        cycle_runner=overrunning_cycle,
        should_continue=three,
    )

    titles = [r["title"] for r in _records(lake_root)]
    assert any("XYZ" in title for title in titles), "the kept ticker stopped being charged"
    assert not any("ZZZ" in title for title in titles), "the retired ticker was still charged"


def test_a_roster_that_will_not_load_at_start_says_which_guards_it_left_off(tmp_path, capsys):
    """A guard that fails to build is off for the life of the process, so it must say so.

    The three builders run once, before the loop, and are never rebuilt. The cycle
    runner re-reads the roster every minute, so an operator who fixes the file before
    the open leaves the daemon capturing all session with gap marking, the close guard,
    the watchdog, and the dead-man all off. The dead-man going unfed does page, but it
    pages capture-down while data is landing. Only this line names the real cause.
    """
    from lake import daemon
    from lake.capture import CycleResult
    from tests.support.calendar import et, weekday_sessions
    from tests.support.clock import ManualClock
    from tests.support.config import write_config
    from tests.support.pinger import FakePinger

    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("XYZ: {options: fal")

    clock = ManualClock(start=et(2026, 9, 2, 9, 28))
    ticks = [0]

    def two() -> bool:
        ticks[0] += 1
        # The operator's editor finishes the save before the first capture slot.
        tickers.write_text("XYZ: {options: false}\n")
        return ticks[0] <= 2

    daemon.run_loop_from_config(
        config_path=str(config),
        tickers_path=str(tickers),
        clock=clock,
        calendar=weekday_sessions(date(2026, 8, 31)),
        assertion_runner=lambda args: None,
        transport=Broken(),
        pinger=FakePinger(),
        cycle_runner=lambda *, close_tag, session_phase: CycleResult(et(2026, 9, 2, 9, 30), ()),
        should_continue=two,
    )

    err = capsys.readouterr().err
    # Every guard that failed to build names itself and the file that stopped it.
    for guard in ("gap marking off", "close guard off", "the alarm off"):
        assert guard in err, f"a session ran with {guard.removesuffix(' off')} off and said nothing"
    assert "tickers file is not valid YAML" in err
