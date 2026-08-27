"""The launchd plist generator, decided from values alone.

These pin the two traps the design's enforcement note calls out. The schedule must be
emitted as launchd's integer ``Hour``/``Minute``, never a ``"HH:MM"`` string, and no
machine path may be baked in: every path is a value the caller supplied. So the tier is
unit. The generator builds a plist string from arguments and nothing crosses a boundary.
"""

from __future__ import annotations

import plistlib

import pytest

from lake import runner


def test_daily_job_emits_integer_hour_and_minute():
    job = runner.daily_runner_job(python="/opt/py/bin/python", hour=16, minute=10)
    interval = job.to_dict()["StartCalendarInterval"]
    assert interval == {"Hour": 16, "Minute": 10}
    # Integers, not strings. A regression to a "HH:MM" literal would be a string here
    # and would trip the session-time enforcement scanner in source.
    assert isinstance(interval["Hour"], int)
    assert isinstance(interval["Minute"], int)


def test_daily_job_runs_the_runner_module():
    job = runner.daily_runner_job(python="/opt/py/bin/python", hour=16, minute=10)
    assert job.to_dict()["ProgramArguments"] == ["/opt/py/bin/python", "-m", "lake.runner", "run"]
    assert job.label == runner.DAILY_LABEL


def test_daily_job_renders_valid_plist_xml():
    job = runner.daily_runner_job(python="/opt/py/bin/python", hour=16, minute=10)
    parsed = plistlib.loads(job.render().encode("utf-8"))
    assert parsed["Label"] == runner.DAILY_LABEL
    assert parsed["StartCalendarInterval"]["Hour"] == 16
    assert parsed["StartCalendarInterval"]["Minute"] == 10


def test_config_path_rides_the_environment_not_the_arguments():
    job = runner.daily_runner_job(
        python="/opt/py/bin/python",
        hour=16,
        minute=10,
        config_path="/Users/someone/.config/marketlake/config.yaml",
    )
    plist = job.to_dict()
    # The machine path is a caller-supplied value carried in the environment, never a
    # tracked literal and never mixed into the program arguments.
    assert plist["EnvironmentVariables"]["MARKETLAKE_CONFIG"].endswith("config.yaml")
    assert "/Users/someone" not in " ".join(plist["ProgramArguments"])


def test_measurement_job_is_an_array_of_per_minute_intervals():
    job = runner.measurement_runner_job(
        python="/opt/py/bin/python",
        start_hour=9,
        start_minute=30,
        end_hour=9,
        end_minute=32,
    )
    intervals = job.to_dict()["StartCalendarInterval"]
    assert intervals == [
        {"Hour": 9, "Minute": 30},
        {"Hour": 9, "Minute": 31},
        {"Hour": 9, "Minute": 32},
    ]


def test_minute_intervals_cross_the_hour_boundary():
    intervals = runner.minute_intervals(9, 59, 10, 1)
    assert intervals == [
        {"Hour": 9, "Minute": 59},
        {"Hour": 10, "Minute": 0},
        {"Hour": 10, "Minute": 1},
    ]


def test_calendar_interval_rejects_out_of_range():
    with pytest.raises(ValueError):
        runner.calendar_interval(24, 0)
    with pytest.raises(ValueError):
        runner.calendar_interval(9, 60)


def test_minute_intervals_reject_backward_window():
    with pytest.raises(ValueError):
        runner.minute_intervals(10, 0, 9, 0)


def test_plist_cli_prints_xml(capsys):
    code = runner.main(
        ["plist", "daily", "--python", "/opt/py/bin/python", "--hour", "16", "--minute", "10"]
    )
    assert code == 0
    out = capsys.readouterr().out
    parsed = plistlib.loads(out.encode("utf-8"))
    assert parsed["StartCalendarInterval"] == {"Hour": 16, "Minute": 10}
