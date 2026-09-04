"""The control plane's launchd plists, decided from values alone.

Two resident processes render under ``KeepAlive`` with no calendar interval. Two
calendar jobs render the weekday and Sunday intervals. Every path and account is a
value the test supplied, never a tracked literal. And the slice-1 daily job renders
exactly as it did before the ``LaunchdJob`` extension.
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
    path_dirs=("/Users/someone/.local/bin",),
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
    assert env["PATH"].startswith("/Users/someone/.local/bin:")
    assert "/usr/bin" in env["PATH"].split(":")
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
    plist = _parsed(cp.sunday_job(HOST, token))
    # The job names the same token file the Time Machine exclusion protects.
    assert plist["ProgramArguments"][-4:] == ["lake.control_plane", "sunday", "--token", token]
    assert plist["StartCalendarInterval"] == {"Weekday": 0, "Hour": 20, "Minute": 0}
    assert "KeepAlive" not in plist


def test_all_jobs_are_the_four_and_carry_no_vendor_sweep():
    labels = [job.label for job in cp.all_jobs(HOST, cp.default_token_path(HOST.home))]
    assert labels == [cp.DAEMON_LABEL, cp.DASHBOARD_LABEL, cp.SELF_CHECK_LABEL, cp.SUNDAY_LABEL]
    assert not any("sweep" in label for label in labels)


def test_intervals_are_integers_not_strings():
    for job in cp.all_jobs(HOST, cp.default_token_path(HOST.home)):
        interval = job.to_dict().get("StartCalendarInterval")
        if interval is None:
            continue
        entries = interval if isinstance(interval, list) else [interval]
        for entry in entries:
            assert all(isinstance(value, int) for value in entry.values())


# -- the LaunchdJob extension -------------------------------------------------------


def test_daily_runner_job_renders_as_before():
    job = runner.daily_runner_job(python="/opt/py/bin/python", hour=16, minute=10)
    assert job.to_dict() == {
        "Label": runner.DAILY_LABEL,
        "ProgramArguments": ["/opt/py/bin/python", "-m", "lake.runner", "run"],
        "StartCalendarInterval": {"Hour": 16, "Minute": 10},
        "RunAtLoad": False,
    }
    assert "KeepAlive" not in job.render()


def test_a_job_that_could_never_start_is_refused():
    with pytest.raises(ValueError):
        runner.LaunchdJob(label="x", program_arguments=("/py",))


def test_run_at_load_alone_is_enough_to_omit_the_interval():
    job = runner.LaunchdJob(label="x", program_arguments=("/py",), run_at_load=True)
    plist = job.to_dict()
    assert "StartCalendarInterval" not in plist
    assert plist["RunAtLoad"] is True
    assert "KeepAlive" not in plist
