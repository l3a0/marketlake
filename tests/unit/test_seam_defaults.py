"""No library entry may default a live seam.

This is the rule, stated once, where a signature change cannot slip past it. Two entries
used to default their seams to the real objects, and a caller that passed none received a
real healthchecks GET, a real ntfy POST, and a real ``rsync``. The caller that most needed
to be asked was a test, and two of them fed the owner's live ``capture`` dead-man six times
per suite run.

Requiring the seams fixed that, but a required argument is enforced only by the signature.
A later edit re-adding ``= None`` and the ``x if x is not None else Live()`` fallback would
restore the bug with every test still green. These assertions are what goes red instead.

The rule is scoped to the entries below on purpose. A ``main`` builds live objects because
that is its job, so ``compact.main``, ``probe_calendar.main`` and ``control_plane.main``
are outside it.
"""

from __future__ import annotations

import inspect

import pytest

from lake import daemon, runner

# Each row is an entry and the seams it must never default. A seam is a parameter whose
# production value reaches past this process.
ENTRIES = [
    (daemon.run_loop_from_config, "transport"),
    (daemon.run_loop_from_config, "pinger"),
    (daemon._alarm, "transport"),
    (daemon._alarm, "pinger"),
    (runner.run_once_from_config, "pinger"),
    (runner.run_once_from_config, "backup"),
]


@pytest.mark.parametrize(("entry", "seam"), ENTRIES, ids=lambda v: getattr(v, "__name__", v))
def test_a_library_entry_never_defaults_a_live_seam(entry, seam):
    parameter = inspect.signature(entry).parameters[seam]
    assert parameter.default is inspect.Parameter.empty, (
        f"{entry.__name__}() defaults its {seam!r} seam. A caller that passes none gets "
        "the live object, which is how the suite came to ping the owner's live check."
    )


def test_omitting_a_seam_is_a_type_error_not_a_live_object():
    # The signature assertion above states the rule. This one shows what a caller sees,
    # because a default of `None` that raises later would satisfy the rule and still be
    # a worse failure than the call simply not being made.
    with pytest.raises(TypeError, match="transport"):
        daemon.run_loop_from_config(config_path="/nonexistent.yaml")
    with pytest.raises(TypeError, match="pinger"):
        runner.run_once_from_config(config_path="/nonexistent.yaml")
