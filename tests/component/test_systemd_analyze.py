"""The rendered units, read by systemd's own analyzer rather than by a test's parser.

The other systemd tests read the unit files the way this repository writes them. Only
systemd can say whether it reads them the same way, so these hand the render to
``systemd-analyze``. The tool exists on CI's Linux runner and on no Mac, so each test
skips where it is absent and fails where it is absent under ``CI``, through
``tests.support.real_tools``.

1. ``verify`` reads every unit. It fails on an ``ExecStart=`` binary that does not exist,
   so the render uses this test's own interpreter and a checkout and home under
   ``tmp_path``. ``--recursive-errors=no`` keeps it to these units, and without it a
   misplaced key exits 0. ``--man=no`` skips the man-page lookup. The stop's unit runs
   the checkout's ``deploy/vm-stop.sh``, so the checkout holds an executable file there.
2. ``calendar`` computes each timer's next elapses under an explicit ``TZ`` across the
   2026-11-01 clock change, and every one must land on the job's Eastern wall-clock time.

CI's runner moves to a newer systemd than the VM's 255 in late 2026. The units use only
settings older than 255, so the runner is not pinned.
"""

from __future__ import annotations

import getpass
import os
import re
import subprocess
import sys
from datetime import UTC, datetime, timedelta

import pytest

from lake import control_plane as cp
from lake.calendar import MARKET_TZ
from tests.support.real_tools import require_tool

ANALYZE = "systemd-analyze"


def test_systemd_reads_every_rendered_unit(tmp_path):
    tool = require_tool(ANALYZE)
    checkout = tmp_path / "checkout"
    home = tmp_path / "home"
    lake = tmp_path / "lake"
    for path in (checkout, home, lake):
        path.mkdir()
    stop = checkout / cp.STOP_SCRIPT
    stop.parent.mkdir()
    stop.write_text("#!/bin/bash\n")
    stop.chmod(0o755)
    out = tmp_path / "out"
    args = [
        "render",
        "--init",
        "systemd",
        "--out",
        str(out),
        "--python",
        sys.executable,
        "--owner",
        getpass.getuser(),
        "--home",
        str(home),
        "--project-dir",
        str(checkout),
        "--lake-mount",
        str(lake),
    ]
    assert cp.main(args) == 0
    units = sorted(str(path) for path in out.iterdir() if path.suffix in (".service", ".timer"))
    assert len(units) == 14, units
    proc = subprocess.run(
        [tool, "verify", "--recursive-errors=no", "--man=no", *units],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


# The analyzer's elapse lines. The first reads ``Next elapse:``. systemd-analyze(1)'s own
# example shows the later ones as ``Iter. #N:``, and ``Iteration #N:`` is accepted too,
# since a count check below fails loudly if neither matches. Under ``TZ=UTC`` each line
# prints its instant in UTC.
_ELAPSE = re.compile(
    r"^\s*(?:Next elapse|Iter(?:\.|ation) #\d+): \w{3} "
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) UTC$",
    re.MULTILINE,
)

_EASTERN_DAYLIGHT = timedelta(hours=-4)
_EASTERN_STANDARD = timedelta(hours=-5)


@pytest.mark.parametrize(
    ("label", "base", "iterations"),
    [
        # Thursday 29 October to Thursday 5 November: two daylight weekdays, then four
        # standard ones after the Sunday the clocks go back.
        (cp.SELF_CHECK_LABEL, "2026-10-29 00:00:00 UTC", 6),
        (cp.CALENDAR_PROBE_LABEL, "2026-10-29 00:00:00 UTC", 6),
        (cp.EOD_SWEEP_LABEL, "2026-10-29 00:00:00 UTC", 6),
        # 25 October in daylight time, then 1 and 8 November in standard time.
        (cp.SUNDAY_LABEL, "2026-10-20 00:00:00 UTC", 3),
        # The hosted VM's morning ping, a systemd-only unit (#868).
        (cp.VM_UP_LABEL, "2026-10-29 00:00:00 UTC", 6),
    ],
)
def test_each_timer_keeps_its_eastern_time_across_the_clock_change(label, base, iterations):
    tool = require_tool(ANALYZE)
    host = cp.SystemdHost(python="/py", owner="someone", home="/h", project_dir="/p")
    timed = [*cp.systemd_units(host), cp.vm_up_job(host)]
    (unit,) = [unit for unit in timed if unit.label == label]
    assert unit.schedule is not None
    proc = subprocess.run(
        [
            tool,
            "calendar",
            f"--base-time={base}",
            f"--iterations={iterations}",
            cp.on_calendar(unit.schedule),
        ],
        env={**os.environ, "TZ": "UTC"},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    elapses = [
        datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        for stamp in _ELAPSE.findall(proc.stdout)
    ]
    assert len(elapses) == iterations, proc.stdout
    offsets = set()
    for elapse in elapses:
        eastern = elapse.astimezone(MARKET_TZ)
        assert (eastern.hour, eastern.minute, eastern.second) == (
            unit.schedule.at.hour,
            unit.schedule.at.minute,
            0,
        ), (elapse, proc.stdout)
        assert eastern.weekday() in unit.schedule.days, (elapse, proc.stdout)
        offsets.add(eastern.utcoffset())
    # Both sides of the change were checked, not one side twice.
    assert offsets == {_EASTERN_DAYLIGHT, _EASTERN_STANDARD}, proc.stdout
