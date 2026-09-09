"""The two producers the Sunday job's seams get in production.

``token_canary`` is the throwaway authenticated call. ``reminder_publisher`` is the
re-auth reminder's delivery. Both decide from values alone, so both are unit tests: the
vendor is a fake, the publisher is a recorder, and the clock is manual. Nothing here
builds a real client and nothing reaches the network.

The rules under test. The canary answers True only when a call was made and came back,
so a dead token, an unreachable vendor and a non-2xx all answer False and none of them
raises out of the job. The vendor is rebuilt on every call, which is what lets a retry
see a re-login the evening's first attempt could not. The reminder goes out at the
design's priority and title, stamped from the injected clock, and a delivery that fails
leaves the sink returning normally.
"""

from __future__ import annotations

from datetime import datetime

from lake.alert import Delivery
from lake.control_plane import (
    CANARY_SYMBOL,
    REMINDER_EVENT,
    REMINDER_PRIORITY,
    REMINDER_TITLE,
    ReauthReminder,
    reminder_publisher,
    token_canary,
)
from lake.schwab import VendorAuthError
from lake.vendor import VendorResponse
from tests.support.calendar import et
from tests.support.clock import ManualClock

TOKEN = "/nowhere/token.json"
REMINDER = ReauthReminder(
    title=REMINDER_TITLE,
    body="The coverage assertion failed. Token minted 2026-08-23.",
    priority=REMINDER_PRIORITY,
)
SUNDAY_20 = et(2026, 8, 30, 20, 0)


class _Vendor:
    """A vendor whose one quote call is canned. It records what it was asked."""

    def __init__(self, *, status: int = 200, raises: Exception | None = None) -> None:
        self.status = status
        self.raises = raises
        self.asked: list[list[str]] = []

    def get_quotes(self, symbols):
        self.asked.append(list(symbols))
        if self.raises is not None:
            raise self.raises
        return VendorResponse(status=self.status, body={})


class _Factory:
    """A vendor factory recording every build, so a test can count them."""

    def __init__(self, vendor: _Vendor) -> None:
        self.vendor = vendor
        self.builds: list[tuple[object, str, str]] = []

    def __call__(self, token_path, *, api_key, app_secret):
        self.builds.append((token_path, api_key, app_secret))
        return self.vendor


def _canary(vendor: _Vendor, **kwargs):
    factory = _Factory(vendor)
    call = token_canary(
        token_path=TOKEN,
        api_key="api-key",
        app_secret="app-secret",
        vendor_factory=factory,
        **kwargs,
    )
    return call, factory


# -- the canary ----------------------------------------------------------------------


def test_a_call_that_comes_back_passes_and_quotes_one_symbol():
    vendor = _Vendor()
    call, factory = _canary(vendor)
    assert call() is True
    assert vendor.asked == [[CANARY_SYMBOL]]
    assert factory.builds == [(TOKEN, "api-key", "app-secret")]


def test_a_dead_token_answers_false_rather_than_raising():
    # The dead refresh token's second shape: the refresh fails and no request is made.
    # The vendor names it ``VendorAuthError``. A raise here would end the evening, so
    # the scrub, the alarm read-back and the check's ping would go with it.
    call, _ = _canary(_Vendor(raises=VendorAuthError("refresh failed")))
    assert call() is False


def test_an_unreachable_vendor_answers_false():
    # A call that did not come back has proved nothing, and the canary exists to prove
    # capture still works. The half-hour retry is what absorbs a transient outage.
    call, _ = _canary(_Vendor(raises=TimeoutError("timed out")))
    assert call() is False


def test_a_client_that_cannot_be_built_answers_false():
    def refuse(token_path, *, api_key, app_secret):
        raise OSError("token file vanished")

    call = token_canary(
        token_path=TOKEN, api_key="api-key", app_secret="app-secret", vendor_factory=refuse
    )
    assert call() is False


def test_a_non_success_status_answers_false():
    # The dead token's other shape arrives as a status rather than as a raise.
    for status in (401, 403, 500):
        call, _ = _canary(_Vendor(status=status))
        assert call() is False, status


def test_the_failure_class_reaches_the_job_log_and_the_message_does_not(capsys):
    # The class tells a dead token from a dead network. The text is the library's and
    # may have been built from a credential, so it stays out of anything a person reads.
    call, _ = _canary(_Vendor(raises=VendorAuthError("app-secret was rejected")))
    call()
    printed = capsys.readouterr().out
    assert "sunday: canary call failed: VendorAuthError" in printed
    assert "app-secret" not in printed


def test_every_call_builds_the_vendor_again():
    # The 21:00 attempt has to see a re-login done at 20:40. A client built once would
    # keep calling on the token the evening started with.
    vendor = _Vendor()
    call, factory = _canary(vendor)
    call()
    call()
    assert len(factory.builds) == 2
    assert vendor.asked == [[CANARY_SYMBOL], [CANARY_SYMBOL]]


# -- the reminder's delivery ---------------------------------------------------------


class _Recorder:
    """A publisher recording each message and the instant it was stamped with."""

    def __init__(self, delivery) -> None:
        self.delivery = delivery
        self.published: list[tuple[object, datetime]] = []

    def publish(self, message, *, now):
        self.published.append((message, now))
        return self.delivery


def test_the_sink_publishes_the_reminder_in_the_shape_the_design_pins():
    publisher = _Recorder(Delivery(sent=True))
    sink = reminder_publisher(publisher=publisher, clock=ManualClock(start=SUNDAY_20))
    sink(REMINDER)
    (message, now) = publisher.published[0]
    assert message.event == REMINDER_EVENT
    assert message.title == REMINDER_TITLE
    assert message.body == REMINDER.body
    # Priority 3 is the reminder tier. A 5 would ring the phone like an auth-death page.
    assert message.priority == REMINDER_PRIORITY
    # Stamped from the job's own clock, so the record names the attempt that raised it.
    assert now == SUNDAY_20


def test_a_reminder_that_did_not_go_is_logged_and_the_sink_returns(capsys):
    # The publisher is total, so a failed push is a return value rather than a raise.
    # The evening's retries carry on, which is what keeps an unreachable ntfy from
    # costing the scrub and the check's ping.
    publisher = _Recorder(Delivery(sent=False, reason="post_failed", recorded=True))
    sink = reminder_publisher(publisher=publisher, clock=ManualClock(start=SUNDAY_20))
    sink(REMINDER)
    assert "sunday: reminder not sent: post_failed, written down" in capsys.readouterr().out


def test_a_reminder_lost_twice_says_so(capsys):
    publisher = _Recorder(Delivery(sent=False, reason="post_failed", recorded=False))
    sink = reminder_publisher(publisher=publisher, clock=ManualClock(start=SUNDAY_20))
    sink(REMINDER)
    assert "sunday: reminder not sent: post_failed, lost" in capsys.readouterr().out


def test_a_delivered_reminder_prints_nothing(capsys):
    publisher = _Recorder(Delivery(sent=True))
    sink = reminder_publisher(publisher=publisher, clock=ManualClock(start=SUNDAY_20))
    sink(REMINDER)
    assert capsys.readouterr().out == ""
