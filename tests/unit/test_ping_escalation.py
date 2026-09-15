"""A ping healthchecks refused, and the page that breaks its silence.

A slug with no row pings nothing. Its only symptom is silence, and silence is already
what the check exists to report, so no check can ever report this one. The compaction
job ran that way from the day it landed. It sealed twenty-one partitions and 9.8 million
rows, reported ``pinged=False`` on one line among twenty-one successful seals, and the
dead-man meant to page when compaction stops had nothing to go silent from.

What tells the two failures apart is already in the failure. healthchecks answers a ping
it cannot resolve with a status, and ``urlopen`` raises that as ``urllib.error.HTTPError``,
a ``URLError`` subclass carrying ``.code``. A ping lost in transport carries no status at
all. That is why the compaction run printed ``ping failed: HTTPError`` rather than a
transport error name.

So a refusal pages and a transport failure does not. The design already answers the
second one: the dead-man grace is set looser than the watchdog's on purpose, so a wifi
blip drops pings while capture keeps journaling locally, and healthchecks pages from
outside if the outage lasts. Paging from the laptop would fail in that same outage.
"""

from __future__ import annotations

import http.client
import inspect
import urllib.error
from datetime import datetime
from pathlib import Path

import pytest

from lake.alert import CAP_REACHED, PAGE_PRIORITY, POST_FAILED, Delivery, Publisher
from lake.calendar import MARKET_TZ
from lake.compact import COMPACTION_SLUG
from lake.control_plane import CALENDAR_PROBE_SLUG, PRE_OPEN_SLUG, SUNDAY_SLUG
from lake.deadman import CAPTURE_SLUG
from lake.runner import (
    PING_FAILURES,
    PING_REFUSED_EVENT,
    PING_REFUSED_TITLE,
    SLICE1_RUNNER_SLUG,
    SlugEscalation,
    escalate_ping_failure,
    ping_refused_page,
    refused_status,
    run_once,
)
from tests.support.config import NTFY_TOPIC, PING_KEY, write_config
from tests.support.transport import FakeTransport

NOW = datetime(2026, 9, 14, 16, 30, tzinfo=MARKET_TZ)

# The five checks a live job feeds. The retired slice-1 slug is deliberately absent, and
# the test at the bottom of this file is what keeps it absent.
LIVE_SLUGS = (
    COMPACTION_SLUG,
    CALENDAR_PROBE_SLUG,
    PRE_OPEN_SLUG,
    SUNDAY_SLUG,
    CAPTURE_SLUG,
)


def _refused(status: int = 404, slug: str = COMPACTION_SLUG) -> urllib.error.HTTPError:
    """What ``urlopen`` raises when healthchecks answers a ping it cannot resolve."""
    return urllib.error.HTTPError(
        f"https://hc-ping.com/{PING_KEY}/{slug}", status, "Not Found", {}, None
    )


class _Sink:
    """A publisher recording each page. The real one POSTs to ntfy.

    It answers a ``Delivery`` because the real one does, and because what that object
    says is now load-bearing: a page that did not reach the phone must not spend the
    once-per-slug guard. A fake answering ``None`` would hide that.
    """

    def __init__(self, delivery: Delivery | None = None) -> None:
        self.sent: list = []
        self.moments: list[datetime] = []
        self._delivery = Delivery(True) if delivery is None else delivery

    def publish(self, message, *, now):
        self.sent.append(message)
        self.moments.append(now)
        return self._delivery


# -- telling a refusal from a ping that never landed --------------------------


def test_a_refused_ping_carries_the_status_that_names_it():
    assert refused_status(_refused()) == 404
    # A refusal is caught by the same tuple every call site already catches, which is
    # why it reached a log line instead of a page.
    assert isinstance(_refused(), PING_FAILURES)


@pytest.mark.parametrize(
    "exc",
    [
        urllib.error.URLError(TimeoutError("timed out")),
        urllib.error.URLError(OSError("connection refused")),
        TimeoutError("timed out"),
        OSError("no route to host"),
        http.client.BadStatusLine("garbage"),
    ],
    ids=["url_timeout", "url_refused", "timeout", "oserror", "malformed"],
)
def test_a_ping_lost_in_transport_carries_no_status(exc):
    assert refused_status(exc) is None


@pytest.mark.parametrize("status", [500, 502, 503])
def test_a_healthchecks_outage_stays_on_the_transport_side(status):
    # A 5xx is healthchecks failing rather than refusing. The row may well exist, and
    # the next run reaches it. Paging would turn their outage into ours.
    assert refused_status(_refused(status)) is None


def test_the_rate_limit_is_not_a_missing_row():
    # healthchecks answers 429 when one check is pinged more than five times a minute.
    # That says the row exists and is being fed too fast, which is the opposite of the
    # failure here, and it clears on its own. A page would name a repair nobody owes.
    assert refused_status(_refused(429)) is None


@pytest.mark.parametrize("status", [400, 404, 409])
def test_a_status_healthchecks_would_not_record_pages(status):
    # 404 is a slug with no row, 409 a slug matching more than one, and 400 a malformed
    # ping URL. None feeds a check, none clears on its own, and no check reports any of
    # them because no check is being fed.
    assert refused_status(_refused(status)) == status


def test_the_page_names_no_repair_it_cannot_know_is_owed():
    # Three statuses reach the page and their repairs differ. A body reading "create the
    # row" would send the operator after a row that already exists when the answer was a
    # 409, so the body states the effect instead.
    body = ping_refused_page(SUNDAY_SLUG, 409).body
    assert "409" in body and SUNDAY_SLUG in body
    assert "create" not in body.lower()


# -- the page itself ----------------------------------------------------------


def test_a_refused_ping_pages_and_the_page_names_the_slug():
    sink = _Sink()
    assert escalate_ping_failure(_refused(), slug=COMPACTION_SLUG, publisher=sink, now=NOW)
    assert len(sink.sent) == 1
    page = sink.sent[0]
    assert page.event == PING_REFUSED_EVENT
    assert page.title == PING_REFUSED_TITLE
    assert COMPACTION_SLUG in page.body
    assert "404" in page.body
    # The page tier is the design's, not a new one.
    assert page.priority == PAGE_PRIORITY
    assert sink.moments == [NOW]


@pytest.mark.parametrize(
    "exc",
    [TimeoutError("timed out"), OSError("no route to host")],
    ids=["timeout", "oserror"],
)
def test_a_ping_lost_in_transport_pages_nothing(exc):
    sink = _Sink()
    assert not escalate_ping_failure(exc, slug=COMPACTION_SLUG, publisher=sink, now=NOW)
    assert sink.sent == []


def test_a_page_that_never_left_the_laptop_leaves_the_slug_armed():
    """ntfy down at the moment of the first refusal must not silence the slug for good.

    The guard is spent by a page that reached the phone, never by one that was only
    attempted. ``DeadMan`` keeps its guard for the daemon's whole life and re-arms only
    on a ping that lands, which by construction never happens while the row is missing.
    So spending the guard on a failed POST would lose the page until the daemon restarts.
    """
    down = _Sink(Delivery(False, POST_FAILED, recorded=True))
    escalation = SlugEscalation(down)
    assert not escalation.failed(_refused(), slug=CAPTURE_SLUG, now=NOW)
    assert len(down.sent) == 1
    # ntfy comes back, and the next refusal reaches the phone.
    down._delivery = Delivery(True)
    assert escalation.failed(_refused(), slug=CAPTURE_SLUG, now=NOW)
    assert not escalation.failed(_refused(), slug=CAPTURE_SLUG, now=NOW)
    assert len(down.sent) == 2


def test_a_page_the_cap_swallowed_also_leaves_the_slug_armed():
    # The day's cap is the publisher's own decision and it turns over with the date, so
    # the same rule applies: nobody was told, so nothing is spent.
    capped = _Sink(Delivery(False, CAP_REACHED, recorded=True))
    escalation = SlugEscalation(capped)
    assert not escalation.failed(_refused(), slug=CAPTURE_SLUG, now=NOW)
    assert not escalation.failed(_refused(), slug=CAPTURE_SLUG, now=NOW)
    assert len(capped.sent) == 2


def test_a_page_that_did_not_go_is_named_on_stderr(capsys):
    # A page lost quietly is invisible, which is the failure this module exists to stop.
    # The assertion page and the Sunday reminder already name theirs the same way.
    escalate_ping_failure(
        _refused(),
        slug=CAPTURE_SLUG,
        publisher=_Sink(Delivery(False, POST_FAILED, recorded=True)),
        now=NOW,
    )
    printed = capsys.readouterr().err
    assert CAPTURE_SLUG in printed and POST_FAILED in printed
    assert PING_KEY not in printed


def test_a_producer_with_no_publisher_pages_nothing():
    # The default is None rather than a live object, so a caller that omits one can
    # never reach a real phone.
    assert not escalate_ping_failure(_refused(), slug=COMPACTION_SLUG, publisher=None, now=NOW)


# -- once on the transition ---------------------------------------------------


def test_a_second_refusal_of_the_same_slug_pages_nothing():
    # A missing row is refused on every run forever. Paging each time would empty the
    # phone's attention for the one page that matters.
    sink = _Sink()
    escalation = SlugEscalation(sink)
    assert escalation.failed(_refused(), slug=CAPTURE_SLUG, now=NOW)
    assert not escalation.failed(_refused(), slug=CAPTURE_SLUG, now=NOW)
    assert not escalation.failed(_refused(), slug=CAPTURE_SLUG, now=NOW)
    assert len(sink.sent) == 1


def test_a_landed_ping_re_arms_the_slug():
    # The row was created by hand and armed, and the pings started landing. A refusal
    # after that is a new failure and has to page again.
    sink = _Sink()
    escalation = SlugEscalation(sink)
    assert escalation.failed(_refused(), slug=CAPTURE_SLUG, now=NOW)
    escalation.landed(CAPTURE_SLUG)
    assert escalation.failed(_refused(), slug=CAPTURE_SLUG, now=NOW)
    assert len(sink.sent) == 2


def test_one_slug_that_paged_leaves_another_armed():
    sink = _Sink()
    escalation = SlugEscalation(sink)
    assert escalation.failed(_refused(slug=CAPTURE_SLUG), slug=CAPTURE_SLUG, now=NOW)
    assert escalation.failed(_refused(slug=SUNDAY_SLUG), slug=SUNDAY_SLUG, now=NOW)
    assert [page.body.split(":")[0] for page in sink.sent] == [CAPTURE_SLUG, SUNDAY_SLUG]


def test_a_transport_failure_never_spends_the_one_page():
    # A wifi blip must not leave a later refusal silent, which is what would happen if
    # the guard were spent by a failure that never paged.
    sink = _Sink()
    escalation = SlugEscalation(sink)
    assert not escalation.failed(OSError("no network"), slug=CAPTURE_SLUG, now=NOW)
    assert escalation.failed(_refused(), slug=CAPTURE_SLUG, now=NOW)
    assert len(sink.sent) == 1


# -- what never reaches a phone -----------------------------------------------


@pytest.mark.parametrize("slug", LIVE_SLUGS)
def test_no_refused_ping_page_carries_the_url_or_its_key(slug: str, tmp_path: Path):
    """The page names the slug and the status, and never the URL, which carries the key.

    The publisher's own refusal is the sweep, so this drives a real publisher built with
    the two values that must never reach a phone. A page carrying either is refused
    rather than sent, so ``sent`` is the whole assertion.
    """
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    write_config(tmp_path, lake_root)
    transport = FakeTransport()
    publisher = Publisher(lake_root=lake_root, transport=transport, secrets=(PING_KEY, NTFY_TOPIC))
    url = f"https://hc-ping.com/{PING_KEY}/{slug}"
    page = ping_refused_page(slug, 404)
    assert PING_KEY not in page.body and PING_KEY not in page.title
    assert url not in page.body and url not in page.title
    assert publisher.publish(page, now=NOW).sent
    assert transport.messages == [page]


# -- the site that must be left alone ------------------------------------------


def test_the_retired_slice_one_check_escalates_nothing():
    """``slice1-capture`` is the sixth ping site and the one that must stay silent.

    Its healthchecks row was deleted on purpose when the slice-2 checks superseded it,
    so a refusal there is the expected answer rather than a finding. Escalating would
    page on every run to announce that a retired check is missing. The leftover itself
    is issue #208 and is deferred, so the runner keeps no publisher at all.
    """
    assert SLICE1_RUNNER_SLUG not in LIVE_SLUGS
    assert "publisher" not in inspect.signature(run_once).parameters
