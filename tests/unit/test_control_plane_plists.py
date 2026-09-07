"""The control plane's launchd plists, decided from values alone.

Two resident processes render under ``KeepAlive`` with no calendar interval. Two
calendar jobs render the weekday and Sunday intervals. Every path and account is a
value the test supplied, never a tracked literal.
"""

from __future__ import annotations

import plistlib

import pytest

from lake import control_plane as cp
from lake import runner

HOST = cp.LaunchdHost(
    python="/opt/py/bin/python",
    owner="someone",
    home="/Users/someone",
    project_dir="/Users/someone/marketlake",
    log_dir="/Users/someone/Library/Logs/marketlake",
    config_path="/Users/someone/.config/marketlake/config.yaml",
)


def _parsed(job: runner.LaunchdJob) -> dict:
    return plistlib.loads(job.render().encode("utf-8"))


# -- the resident processes ----------------------------------------------------


@pytest.mark.parametrize(
    ("build", "label", "module"),
    [
        (cp.daemon_job, cp.DAEMON_LABEL, "lake.daemon"),
        (cp.dashboard_job, cp.DASHBOARD_LABEL, "lake.dashboard"),
    ],
)
def test_resident_plists_keep_alive_as_the_owner_with_no_interval(build, label, module):
    plist = _parsed(build(HOST))
    assert plist["Label"] == label
    assert plist["ProgramArguments"] == ["/opt/py/bin/python", "-m", module]
    assert plist["KeepAlive"] is True
    assert plist["RunAtLoad"] is True
    assert plist["UserName"] == "someone"
    assert plist["GroupName"] == "staff"
    assert "StartCalendarInterval" not in plist


def test_resident_plists_carry_the_working_dir_logs_and_environment():
    plist = _parsed(cp.daemon_job(HOST))
    assert plist["WorkingDirectory"] == "/Users/someone/marketlake"
    assert plist["StandardOutPath"].startswith("/Users/someone/Library/Logs/marketlake/")
    assert plist["StandardErrorPath"].startswith("/Users/someone/Library/Logs/marketlake/")
    assert plist["StandardOutPath"] != plist["StandardErrorPath"]
    env = plist["EnvironmentVariables"]
    assert env["HOME"] == "/Users/someone"
    # An equality pin, not a membership one. It fails if any directory is ever
    # prepended again, which is what the dropped --path-dir knob used to do.
    assert env["PATH"].split(":") == ["/usr/bin", "/bin", "/usr/sbin", "/sbin"]
    assert env["MARKETLAKE_CONFIG"].endswith("config.yaml")


def test_the_owner_is_a_parameter_not_a_literal():
    other = cp.LaunchdHost(
        python="/py", owner="alice", home="/h", project_dir="/p", log_dir="/l", group="wheel"
    )
    plist = _parsed(cp.daemon_job(other))
    assert plist["UserName"] == "alice"
    assert plist["GroupName"] == "wheel"


# -- the calendar jobs ------------------------------------------------------------


def test_self_check_fires_weekdays_at_the_pre_open_time():
    plist = _parsed(cp.self_check_job(HOST))
    assert plist["ProgramArguments"] == [
        "/opt/py/bin/python",
        "-m",
        "lake.control_plane",
        "self-check",
    ]
    assert plist["StartCalendarInterval"] == [
        {"Weekday": wd, "Hour": 8, "Minute": 30} for wd in (1, 2, 3, 4, 5)
    ]
    assert "KeepAlive" not in plist


def test_sunday_job_fires_sunday_at_the_maintenance_time():
    token = cp.default_token_path(HOST.home)
    plist = _parsed(cp.sunday_job(HOST))
    # The job names the same token file the Time Machine exclusion protects.
    assert plist["ProgramArguments"][-4:] == ["lake.control_plane", "sunday", "--token", token]
    assert plist["StartCalendarInterval"] == {"Weekday": 0, "Hour": 20, "Minute": 0}
    assert "KeepAlive" not in plist


@pytest.mark.parametrize(
    ("build", "run_at_load"),
    [
        (cp.daemon_job, True),
        (cp.dashboard_job, True),
        (cp.self_check_job, True),
        (cp.sunday_job, False),
    ],
)
def test_run_at_load_is_pinned_per_job(build, run_at_load):
    # The three that should start at load do. The Sunday job does not, because a
    # bootstrap or a boot would otherwise scrub the whole lake and assert coverage on
    # a day the design never asks about, then ping the sunday slug midweek. Its
    # calendar interval, pinned above, is then the only thing that starts it. launchd
    # still fires a missed occurrence on the next wake, which is the Monday backstop,
    # and that coalescing does not depend on RunAtLoad.
    assert _parsed(build(HOST))["RunAtLoad"] is run_at_load


def test_all_jobs_are_the_five_and_carry_no_vendor_sweep():
    labels = [job.label for job in cp.all_jobs(HOST)]
    assert labels == [
        cp.DAEMON_LABEL,
        cp.DASHBOARD_LABEL,
        cp.SELF_CHECK_LABEL,
        cp.CALENDAR_PROBE_LABEL,
        cp.SUNDAY_LABEL,
    ]
    # The 18:30 sweep is slice 3 and is not rendered here.
    assert not any("sweep" in label for label in labels)


def test_the_calendar_probe_runs_on_weekday_mornings_and_not_at_load():
    (probe,) = [j for j in cp.all_jobs(HOST) if j.label == cp.CALENDAR_PROBE_LABEL]
    entries = probe.to_dict()["StartCalendarInterval"]
    assert [e["Weekday"] for e in entries] == [1, 2, 3, 4, 5]
    assert {(e["Hour"], e["Minute"]) for e in entries} == {(9, 35)}
    # A load at any other hour would make one vendor call for a question only 09:35
    # can answer.
    assert probe.to_dict()["RunAtLoad"] is False


def test_intervals_are_integers_not_strings():
    for job in cp.all_jobs(HOST):
        interval = job.to_dict().get("StartCalendarInterval")
        if interval is None:
            continue
        entries = interval if isinstance(interval, list) else [interval]
        for entry in entries:
            assert all(isinstance(value, int) for value in entry.values())
