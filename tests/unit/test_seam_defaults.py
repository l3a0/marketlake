"""No entry may let a live seam ride in on a default.

This is the rule, stated once, where a signature change cannot slip past it. A seam is a
parameter whose production value reaches past this process. A healthchecks GET, an ntfy
POST, an ``rsync``, a vendor quote, and a ``launchctl``, ``pmset`` or ``tmutil`` read are
all seams. Two kinds of entry carry them, and each has its own failure to guard.

A library helper takes its seams and must never default one. ``run_loop_from_config``,
``run_once_from_config``, ``_alarm``, ``compact``, ``self_check`` and ``sunday_run`` each
require the seams they use. A later edit re-adding ``= None`` and the
``x if x is not None else Live()`` fallback would restore the old bug with every test
still green. The REQUIRED table is what goes red instead. Two of these entries fed the
owner's live ``capture`` dead-man six times per suite run before the seams were required.

A ``main`` builds its live seams itself, and that is the rule for a ``main`` too, not an
exemption from it. Tests call these mains dozens of times. A ``main`` that accepted a seam
let a test pass a fake or, worse, forget one and get the live object from inside the place
that looked sanctioned. So a ``main`` must accept none of its leaky seams. The FORBIDDEN
table asserts each named seam is absent from the signature, which is stricter than a
no-default check. A re-added seam fails it in any form, required or defaulted.

``probe_calendar.main`` already took none. ``compact.main`` and ``control_plane.main`` now
build ``rsync``, the ntfy POST, the healthchecks GET, the vendor canary, and the
``launchctl``, ``pmset`` and ``tmutil`` reads internally. ``clock`` and ``calendar`` stay
injectable on both. A system clock and an exchange calendar never reach past this process,
so neither is a seam. A test drives a seam-requiring helper directly, or, to exercise a
``main``, monkeypatches the producer the ``main`` builds and checks the objects it built.
"""

from __future__ import annotations

import inspect

import pytest

from lake import compact, control_plane, daemon, runner

# Each row is an entry and a seam it must never default. Requiring the seam means a caller
# that omits it gets a TypeError, not a live object. The protection follows each seam to
# the helper that now owns it: ``compact`` for backup, ``self_check`` and ``sunday_run``
# for the control-plane seams that used to default on ``main``.
REQUIRED = [
    (daemon.run_loop_from_config, "transport"),
    (daemon.run_loop_from_config, "pinger"),
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
]

# Each row is a ``main`` and a seam it must never accept. A ``main`` builds its live seams
# itself, so the seam is absent from the signature and a caller can neither pass a fake nor
# forget one into the live object. ``control_plane.main`` reaches the ntfy POST through the
# ``Publisher`` it builds, so ``transport`` is forbidden on the ``main`` even though
# ``sunday_run`` takes it by way of ``reminder_sink`` rather than as a named seam.
FORBIDDEN = [
    (compact.main, "backup"),
    (compact.main, "pinger"),
    (control_plane.main, "probe"),
    (control_plane.main, "pinger"),
    (control_plane.main, "schedule_reader"),
    (control_plane.main, "canary"),
    (control_plane.main, "exclusion_reader"),
    (control_plane.main, "transport"),
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
