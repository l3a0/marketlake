"""No entry may let a live seam ride in on a default.

This is the rule, stated once, where a signature change cannot slip past it. A seam is a
parameter whose production value reaches past this process. A healthchecks GET, an ntfy
POST, an ``rsync``, a vendor quote, a ``launchctl``, ``pmset``, ``tmutil``, ``systemctl``
or ``timedatectl`` read, a ``caffeinate`` spawn, and a ``sudo pmset`` write are all seams.
Two kinds of entry carry them, and each has its own failure to guard.

A library helper takes its seams and must never default one. ``run_loop_from_config``,
``run_once_from_config``, ``_alarm``, ``compact``, ``self_check``, ``sunday_run``,
``sunday_maintenance``, ``sweep.sweep`` and ``sweep._friday_wake`` each require the seams
they use. Required means the caller must say, and a schedule reader or setter may be
``None`` on a host with no wake. What a default would add is a caller that said nothing
and got the live ``pmset`` read or ``sudo`` write. A later edit re-adding ``= None`` and the
``x if x is not None else Live()`` fallback would restore the old bug with every test
still green. The REQUIRED table is what goes red instead. Two of these entries fed the
owner's live ``capture`` dead-man six times per suite run before the seams were required.

A ``main`` builds its live seams itself, and that is the rule for a ``main`` too, not an
exemption from it. Tests call these mains dozens of times. A ``main`` that accepted a seam
let a test pass a fake or, worse, forget one and get the live object from inside the place
that looked sanctioned. So a ``main`` must accept none of its leaky seams. The FORBIDDEN
table asserts each named seam is absent from the signature, which is stricter than a
no-default check. A re-added seam fails it in any form, required or defaulted.

``reauth.main`` is the same shape. Its seam is the vendor login flow, which opens a
browser, listens on a local callback port, and talks to Schwab. It builds that itself, so
a test drives the entry with a fake ``schwab`` package in ``sys.modules`` rather than by
handing the entry a flow. Its second seam is the token parameter's AWS client, which
``reauth_from_config`` takes as a factory and ``token_store.push`` and ``token_store.pull``
take as a client and a factory. ``reauth.main`` and ``token_store.main`` build both
themselves, so a test answers the client through botocore's event hooks instead. The put's
client also assumes a role through an STS client private to the build, which no hook on
the SSM client reaches, so a test of the put answers that at an STS server on loopback,
through the ``aws_session.STS_ENDPOINT_URL`` seam. ``vm_config.render`` takes a factory
for its own SSM client and a reader for its instance tag too, and ``vm_config.main``
builds both.

``probe_calendar.main`` already took none. ``compact.main``, ``control_plane.main`` and
``daemon.main`` now build ``rsync``, the ntfy POST, the healthchecks GET, the vendor
canary, the ``launchctl``, ``pmset`` and ``tmutil`` reads, the ``systemctl`` and
``timedatectl`` reads, and the daemon's assertion runner internally. ``daemon.main``
joined them with the close+15 compaction: it spawns that job as its own process, so the
seam is the spawn rather than the ``rsync`` the child goes on to run. ``clock`` and
``calendar`` stay injectable on both. A system clock and an exchange calendar never
reach past this process, so neither is a seam. A test drives a seam-requiring helper
directly, or, to exercise a ``main``, monkeypatches the producer the ``main`` builds and
checks the objects it built. The ntfy and healthchecks senders are the exception to where
that patch goes. Every ``main`` gets them from ``lake.outbox``, which builds them from the
classes on ``lake.alert`` and ``lake.runner``, so a test patches them there.
"""

from __future__ import annotations

import inspect

import pytest

from lake import (
    bucket,
    compact,
    control_plane,
    daemon,
    reauth,
    runner,
    sweep,
    token_store,
    vm_config,
)

# Each row is an entry and a seam it must never default. Requiring the seam means a caller
# that omits it gets a TypeError, not a live object. The protection follows each seam to
# the helper that now owns it: ``compact`` for backup, ``self_check`` and ``sunday_run``
# for the control-plane seams that used to default on ``main``.
REQUIRED = [
    (daemon.run_loop_from_config, "transport"),
    (daemon.run_loop_from_config, "pinger"),
    (daemon.run_loop_from_config, "compaction_runner"),
    (daemon._alarm, "transport"),
    (daemon._alarm, "pinger"),
    (runner.run_once_from_config, "pinger"),
    (runner.run_once_from_config, "backup"),
    (compact.compact, "backup"),
    (control_plane.self_check, "probe"),
    (control_plane.self_check, "pinger"),
    (control_plane.sunday_run, "schedule_reader"),
    (control_plane.sunday_run, "pinger"),
    (control_plane.sunday_run, "canary"),
    (control_plane.sunday_maintenance, "schedule_reader"),
    (sweep.sweep, "schedule_setter"),
    (sweep.sweep, "schedule_reader"),
    (sweep._friday_wake, "schedule_setter"),
    (sweep._friday_wake, "schedule_reader"),
    (reauth.reauth, "login_flow"),
    (reauth.reauth_from_config, "login_flow"),
    (reauth.reauth_from_config, "store_client_factory"),
    (token_store.push, "client"),
    (token_store.pull, "client_factory"),
    (vm_config.render, "client_factory"),
    (vm_config.render, "tag_reader"),
    (bucket.nightly_upload, "client"),
    (bucket.first_upload, "client"),
    (bucket.bucket_scrub, "client"),
    (bucket.BucketBackup, "client"),
]

# Each row is a ``main`` and a seam it must never accept. A ``main`` builds its live seams
# itself, so the seam is absent from the signature and a caller can neither pass a fake nor
# forget one into the live object. ``control_plane.main`` reaches the ntfy POST through the
# ``Publisher`` it builds, so ``transport`` is forbidden on the ``main`` even though
# ``sunday_run`` takes it by way of ``reminder_sink`` rather than as a named seam.
# ``compact.main`` reaches it the same way, through the ``Publisher`` its schema-drift
# page sends on, so both names are forbidden there: the publisher it builds and the
# transport that publisher holds.
FORBIDDEN = [
    (compact.main, "backup"),
    (compact.main, "pinger"),
    (compact.main, "publisher"),
    (compact.main, "transport"),
    (daemon.main, "compaction_runner"),
    (daemon.main, "assertion_runner"),
    (control_plane.main, "probe"),
    (control_plane.main, "pinger"),
    (control_plane.main, "schedule_reader"),
    (control_plane.main, "canary"),
    (control_plane.main, "exclusion_reader"),
    (control_plane.main, "clock_probe"),
    (control_plane.main, "transport"),
    (reauth.main, "login_flow"),
    (reauth.main, "store_client_factory"),
    (token_store.main, "client"),
    (token_store.main, "client_factory"),
    (vm_config.main, "client"),
    (vm_config.main, "client_factory"),
    (vm_config.main, "tag_reader"),
    (bucket.main, "client"),
    (compact.main, "client"),
    (control_plane.main, "bucket_client"),
]


def _entry_id(value: object) -> str:
    """A stable id for a parametrized value.

    Both mains are named ``main``, so a bare ``__name__`` would render two identical ids
    and muddy a failure. The module name disambiguates them. A seam is a plain string and
    passes through unchanged.
    """
    name = getattr(value, "__name__", None)
    if name is None:
        return str(value)
    module = getattr(value, "__module__", "").rsplit(".", 1)[-1]
    return f"{module}.{name}" if module else name


@pytest.mark.parametrize(("entry", "seam"), REQUIRED, ids=_entry_id)
def test_a_library_entry_never_defaults_a_live_seam(entry, seam):
    parameter = inspect.signature(entry).parameters[seam]
    assert parameter.default is inspect.Parameter.empty, (
        f"{_entry_id(entry)}() defaults its {seam!r} seam. A caller that passes none gets "
        "the live object, which is how the suite came to ping the owner's live check."
    )


@pytest.mark.parametrize(("entry", "seam"), FORBIDDEN, ids=_entry_id)
def test_a_main_never_accepts_a_live_seam(entry, seam):
    parameters = inspect.signature(entry).parameters
    assert seam not in parameters, (
        f"{_entry_id(entry)}() accepts a {seam!r} parameter. A main builds its live seams "
        "itself, so a test can neither pass a fake nor forget one into the live object. "
        "Build it inside main and drive the seam-requiring helper directly in tests."
    )


def test_omitting_a_seam_is_a_type_error_not_a_live_object():
    # The signature assertion above states the rule. This one shows what a caller sees,
    # because a default of `None` that raises later would satisfy the rule and still be
    # a worse failure than the call simply not being made.
    with pytest.raises(TypeError, match="transport"):
        daemon.run_loop_from_config(config_path="/nonexistent.yaml")
    with pytest.raises(TypeError, match="pinger"):
        runner.run_once_from_config(config_path="/nonexistent.yaml")
