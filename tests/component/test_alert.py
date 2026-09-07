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


def test_the_cap_resets_with_the_session_date_not_the_process(tmp_path):
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
    ],
)
def test_a_page_carrying_a_secret_url_is_refused_and_the_record_redacts_it(tmp_path, message):
    transport = Recording()
    publisher = Publisher(lake_root=tmp_path, transport=transport, pid=1)
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
