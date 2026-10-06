"""The shared roster and the systemd units, decided from values alone.

One roster, ``all_jobs``, serves the launchd host and the systemd host. Each job hands
its host a ``Schedule``, and each host converts it to its own form. These tests read the
records and the rendered unit text. Every path and account is a value the test supplied.
"""

from __future__ import annotations

import configparser

import pytest

from lake import control_plane as cp

LAUNCHD = cp.LaunchdHost(
    python="/opt/py/bin/python",
    owner="someone",
    home="/home/someone",
    project_dir="/home/someone/marketlake",
    log_dir="/home/someone/logs",
)

SYSTEMD = cp.SystemdHost(
    python="/opt/py/bin/python",
    owner="someone",
    home="/home/someone",
    project_dir="/home/someone/marketlake",
    lake_mount="/srv/lake",
)

RESIDENTS = (cp.DAEMON_LABEL, cp.DASHBOARD_LABEL)
TIMED = (cp.SELF_CHECK_LABEL, cp.CALENDAR_PROBE_LABEL, cp.SUNDAY_LABEL, cp.EOD_SWEEP_LABEL)


def _units() -> dict[str, cp.SystemdUnit]:
    return {unit.label: unit for unit in cp.systemd_units(SYSTEMD)}


def _parsed(text: str) -> configparser.ConfigParser:
    """A unit file as sections of keys. ``Environment=`` repeats, so it is read apart."""
    parser = configparser.ConfigParser(strict=False, interpolation=None)
    parser.optionxform = str  # keep systemd's key case
    parser.read_string(text)
    return parser


def _environment(text: str) -> list[str]:
    lines = text.splitlines()
    return [line.removeprefix("Environment=") for line in lines if "Environment=" in line]


# -- the schedule ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "days",
    [(), (7,), (-1,), (4, 0), (1, 1)],
    ids=["empty", "eight-day week", "negative", "descending", "repeated"],
)
def test_a_schedule_refuses_days_that_are_not_sorted_distinct_weekdays(days):
    with pytest.raises(ValueError):
        cp.Schedule(cp.WallClockTime(3, 0), days)


def test_launchd_reads_one_day_as_a_bare_dict():
    """A one-element list would change the Sunday plist's bytes."""
    job = LAUNCHD.job("x", "lake.x", calendar=cp.Schedule(cp.WallClockTime(20, 0), (6,)))
    assert job.calendar_interval == {"Hour": 20, "Minute": 0, "Weekday": 0}


def test_launchd_numbers_sunday_zero_and_sorts_by_its_own_numbers():
    """Python's Sunday is 6 and launchd's is 0, so a full week reorders on conversion.

    Sorted by the Python numbers the entries would run Monday to Sunday. launchd's own
    order runs Sunday first, and the plist's bytes follow the order of the list.
    """
    job = LAUNCHD.job(
        "x", "lake.x", calendar=cp.Schedule(cp.WallClockTime(3, 0), (0, 1, 2, 3, 4, 5, 6))
    )
    assert [entry["Weekday"] for entry in job.calendar_interval] == [0, 1, 2, 3, 4, 5, 6]


def test_launchd_ignores_late_run_ok():
    """launchd replays a missed run for every job already, so the flag has no plist key."""
    schedule = cp.Schedule(cp.WallClockTime(3, 0), (6,))
    late = LAUNCHD.job("x", "lake.x", calendar=schedule, late_run_ok=True)
    on_time = LAUNCHD.job("x", "lake.x", calendar=schedule)
    assert late.render() == on_time.render()


# -- one roster ------------------------------------------------------------------------


def test_both_hosts_run_the_same_roster():
    """Label, module, arguments, schedule and residency, job by job.

    The schedule is compared through what each host wrote, with launchd's weekdays read
    back into Python's numbering here rather than through the converter under test.
    """
    launchd = cp.all_jobs(LAUNCHD)
    systemd = cp.all_jobs(SYSTEMD)
    assert [job.label for job in launchd] == [unit.label for unit in systemd]
    for job, unit in zip(launchd, systemd, strict=True):
        assert job.program_arguments == unit.program_arguments, job.label
        assert job.keep_alive == unit.keep_alive, job.label
        if unit.schedule is None:
            assert job.calendar_interval is None, job.label
            continue
        entries = job.calendar_interval
        entries = entries if isinstance(entries, list) else [entries]
        assert sorted((entry["Weekday"] - 1) % 7 for entry in entries) == list(
            unit.schedule.days
        ), job.label
        assert {(entry["Hour"], entry["Minute"]) for entry in entries} == {
            (unit.schedule.at.hour, unit.schedule.at.minute)
        }, job.label


def test_the_residents_are_the_daemon_and_the_dashboard():
    assert [label for label, unit in _units().items() if unit.keep_alive] == list(RESIDENTS)


def test_the_daemon_label_names_the_daemon_unit():
    """``systemctl is-active com.marketlake.daemon`` resolves this unit with no suffix."""
    assert _units()[cp.DAEMON_LABEL].service_name == f"{cp.DAEMON_LABEL}.service"


def test_a_unit_needs_exactly_one_of_a_schedule_or_keep_alive():
    schedule = cp.Schedule(cp.WallClockTime(3, 0), (6,))
    with pytest.raises(ValueError):
        SYSTEMD.job("x", "lake.x")
    with pytest.raises(ValueError):
        SYSTEMD.job("x", "lake.x", calendar=schedule, keep_alive=True)


# -- the services ----------------------------------------------------------------------


@pytest.mark.parametrize("label", RESIDENTS)
def test_a_resident_restarts_forever_ten_seconds_apart(label):
    """``KeepAlive``'s behaviour, with no start limit and no exit status that stops it.

    The daemon exits 2 on a missing config and has to start once the file lands, so no
    status is excluded from the restart.
    """
    text = _units()[label].service()
    unit = _parsed(text)
    assert unit["Service"]["Type"] == "exec"
    assert unit["Service"]["Restart"] == "always"
    assert unit["Service"]["RestartSec"] == "10"
    # systemd 255 reads the start limit from [Unit], and ignores it in [Service].
    assert unit["Unit"]["StartLimitIntervalSec"] == "0"
    assert "StartLimitIntervalSec" not in unit["Service"]
    assert "RestartPreventExitStatus" not in text
    assert unit["Install"]["WantedBy"] == "multi-user.target"


@pytest.mark.parametrize("label", TIMED)
def test_a_timer_run_service_is_a_oneshot_with_nothing_to_enable(label):
    unit = _parsed(_units()[label].service())
    assert unit["Service"]["Type"] == "oneshot"
    assert not unit.has_section("Install")
    assert "Restart" not in unit["Service"]
    assert "StartLimitIntervalSec" not in unit["Unit"]


@pytest.mark.parametrize("label", RESIDENTS + TIMED)
def test_only_the_daemon_survives_a_child_killed_for_memory(label):
    """systemd 255's default ``OOMPolicy=stop`` stops the whole unit on any kill in it.

    The compaction child lives in the daemon's cgroup, so the daemon carries ``continue``.
    Every other unit keeps the default.
    """
    service = _parsed(_units()[label].service())["Service"]
    if label == cp.DAEMON_LABEL:
        assert service["OOMPolicy"] == "continue"
    else:
        assert "OOMPolicy" not in service


@pytest.mark.parametrize("label", RESIDENTS + TIMED)
def test_only_the_daemon_waits_for_the_network(label):
    unit = _parsed(_units()[label].service())["Unit"]
    if label == cp.DAEMON_LABEL:
        assert unit["After"] == "network-online.target"
        assert unit["Wants"] == "network-online.target"
    else:
        assert "After" not in unit and "Wants" not in unit


@pytest.mark.parametrize("label", RESIDENTS + TIMED)
def test_every_service_runs_as_the_owner_from_the_checkout(label):
    text = _units()[label].service()
    service = _parsed(text)["Service"]
    assert service["User"] == "someone"
    # No Group=, so systemd takes the owner's primary group. macOS's staff is absent here.
    assert "Group" not in service
    assert service["WorkingDirectory"] == "/home/someone/marketlake"
    assert service["ExecStart"].split()[:2] == ["/opt/py/bin/python", "-m"]
    assert _environment(text) == ["HOME=/home/someone", "PYTHONUNBUFFERED=1"]
    # PATH is systemd's default, and the config-directory override never reaches a job.
    assert "PATH=" not in text
    assert "MARKETLAKE_CONFIG_DIR" not in text


@pytest.mark.parametrize("label", RESIDENTS + TIMED)
def test_every_service_waits_for_the_lake_mount(label):
    assert _parsed(_units()[label].service())["Unit"]["RequiresMountsFor"] == "/srv/lake"


def test_no_service_waits_for_a_mount_it_was_not_given():
    bare = cp.SystemdHost(python="/py", owner="someone", home="/h", project_dir="/p")
    for unit in cp.systemd_units(bare):
        assert "RequiresMountsFor" not in unit.service(), unit.label


def test_a_config_path_reaches_every_service_as_marketlake_config():
    host = cp.SystemdHost(
        python="/py", owner="someone", home="/h", project_dir="/p", config_path="/etc/ml.yaml"
    )
    for unit in cp.systemd_units(host):
        assert _environment(unit.service()) == [
            "HOME=/h",
            "PYTHONUNBUFFERED=1",
            "MARKETLAKE_CONFIG=/etc/ml.yaml",
        ], unit.label


def test_no_unit_is_sandboxed():
    """The clock check reaches ``systemd-timedated`` over D-Bus, which a sandbox can cut."""
    for unit in cp.systemd_units(SYSTEMD):
        text = unit.service()
        for key in ("Protect", "Private", "NoNewPrivileges", "Restrict", "SystemCallFilter"):
            assert key not in text, (unit.label, key)


# -- the timers ------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("label", "calendar"),
    [
        (cp.SELF_CHECK_LABEL, "Mon..Fri 08:30:00 America/New_York"),
        (cp.CALENDAR_PROBE_LABEL, "Mon..Fri 09:35:00 America/New_York"),
        (cp.EOD_SWEEP_LABEL, "Mon..Fri 18:30:00 America/New_York"),
        (cp.SUNDAY_LABEL, "Sun 20:00:00 America/New_York"),
    ],
)
def test_each_timer_fires_at_its_time_in_the_market_zone(label, calendar):
    """Written out here rather than read from the renderer's constants.

    The host's clock runs in UTC, so a timer without the zone would fire hours early.
    """
    timer = _parsed(_units()[label].timer())["Timer"]
    assert timer["OnCalendar"] == calendar
    assert timer["AccuracySec"] == "1s"


@pytest.mark.parametrize(
    ("label", "persistent"),
    [
        (cp.SELF_CHECK_LABEL, False),
        (cp.CALENDAR_PROBE_LABEL, False),
        (cp.EOD_SWEEP_LABEL, True),
        (cp.SUNDAY_LABEL, True),
    ],
)
def test_only_the_jobs_written_for_a_late_run_replay_a_missed_one(label, persistent):
    """A late self-check or calendar probe would clear the page its missed run owes."""
    timer = _parsed(_units()[label].timer())["Timer"]
    assert timer.get("Persistent") == ("true" if persistent else None)


@pytest.mark.parametrize("label", TIMED)
def test_a_timer_is_enabled_by_timers_target(label):
    assert _parsed(_units()[label].timer())["Install"]["WantedBy"] == "timers.target"


def test_a_resident_has_no_timer():
    with pytest.raises(ValueError):
        _units()[cp.DAEMON_LABEL].timer()


def test_a_full_week_reads_as_one_range():
    assert cp.on_calendar(cp.Schedule(cp.WallClockTime(3, 0), tuple(range(7)))) == (
        "Mon..Sun 03:00:00 America/New_York"
    )


def test_separate_days_stay_separate():
    schedule = cp.Schedule(cp.WallClockTime(3, 5), (0, 2, 3, 6))
    assert cp.on_calendar(schedule) == "Mon,Wed..Thu,Sun 03:05:00 America/New_York"


# -- the values systemd would expand ---------------------------------------------------


FIELDS = ["python", "owner", "home", "project_dir", "config_path", "lake_mount"]


def _host_values() -> dict[str, str]:
    return {
        "python": "/py",
        "owner": "someone",
        "home": "/h",
        "project_dir": "/p",
        "config_path": "/c.yaml",
        "lake_mount": "/srv/lake",
    }


# ``\x1b`` rather than ``\x1f``: Python's ``\s`` matches ``\x1c`` to ``\x1f``, so only a
# control character it does not match shows the refused range reaches past ``\x0f``.
@pytest.mark.parametrize(
    "character", ["%", "$", "'", '"', "\\", " ", "\t", "\n", "\x01", "\x1b", "\x7f"]
)
@pytest.mark.parametrize("field", FIELDS)
def test_a_value_systemd_would_expand_or_split_is_refused(field, character):
    values = _host_values()
    values[field] = f"/a{character}b"
    with pytest.raises(ValueError, match="systemd would expand or split"):
        cp.SystemdHost(**values)


@pytest.mark.parametrize("field", FIELDS)
def test_an_empty_value_is_refused(field):
    """An empty ``User=`` reads to systemd as no user at all, so the job would run as root."""
    values = _host_values()
    values[field] = ""
    with pytest.raises(ValueError, match="is empty"):
        cp.SystemdHost(**values)


def test_the_needrestart_dropin_defers_every_marketlake_unit():
    lines = [line for line in cp.needrestart_dropin().splitlines() if not line.startswith("#")]
    assert lines == ["$nrconf{override_rc}->{qr(^com\\.marketlake\\.)} = 0;"]
