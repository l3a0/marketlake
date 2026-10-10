"""The systemd render and its scripts across one real boundary: the filesystem.

``render --init systemd`` writes fourteen units, three scripts and a needrestart drop-in. The
rendered ``install.sh``, ``uninstall.sh`` and ``restart.sh`` then run for real, as do the
tracked ``deploy/linux-install.sh``, against the fakes in ``tests.support.fake_systemd``
with ``MARKETLAKE_INSTALL_ROOT`` pointed at a temporary directory. Nothing needs root and
nothing reaches the machine's own systemd. The scripts run under ``/bin/bash``, which is
bash 3.2 on a Mac, so the suite also shows they stay valid there.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from lake import control_plane as cp
from lake import deploy_window as dw
from lake.calendar import MARKET_TZ
from lake.paths import TOKEN_FILE, config_dir, default_token_path
from tests.component.test_control_plane_render import (
    EVERY_DAY_AT_THREE,
    RENDER_ARGS,
    SYSTEMD_EXPECTED_FILES,
    SYSTEMD_RENDER_ARGS,
)
from tests.support.fake_bin import install
from tests.support.fake_systemd import install_fakes

REPO_ROOT = Path(__file__).resolve().parents[2]
ENTRY_POINT = REPO_ROOT / "deploy" / "linux-install.sh"

UNITS = sorted(name for name in SYSTEMD_EXPECTED_FILES if name.endswith((".service", ".timer")))
RESIDENTS = ["com.marketlake.daemon.service", "com.marketlake.dashboard.service"]
TIMERS = [name for name in UNITS if name.endswith(".timer")]
PERSISTENT_STAMPS = ["stamp-com.marketlake.eod-sweep.timer", "stamp-com.marketlake.sunday.timer"]
# The one service that runs a tracked script as root rather than Python as the owner.
STOP_SERVICE = "marketlake-stop.service"


def _render(out: Path, *extra: str) -> None:
    assert cp.main(["render", "--out", str(out), *SYSTEMD_RENDER_ARGS, *extra]) == 0


# -- the render's flags ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (
            [a for a in RENDER_ARGS if a not in ("--log-dir", RENDER_ARGS[-1])],
            "render: --init launchd needs --log-dir\n",
        ),
        (
            [*RENDER_ARGS, "--lake-mount", "/srv/lake"],
            "render: --lake-mount applies only to --init systemd, not --init launchd\n",
        ),
        (
            [*SYSTEMD_RENDER_ARGS, "--log-dir", "/var/log/x"],
            "render: --log-dir applies only to --init launchd, not --init systemd\n",
        ),
        (
            [*SYSTEMD_RENDER_ARGS, "--group", "wheel"],
            "render: --group applies only to --init launchd, not --init systemd\n",
        ),
        (
            [*SYSTEMD_RENDER_ARGS, "--log-dir", "/var/log/x", "--group", "wheel"],
            "render: --log-dir and --group apply only to --init launchd, not --init systemd\n",
        ),
    ],
    ids=[
        "launchd without --log-dir",
        "launchd with --lake-mount",
        "--log-dir",
        "--group",
        "both",
    ],
)
def test_a_flag_the_host_has_no_use_for_is_refused(args, message, tmp_path, capsys):
    """One ``render:`` line and exit 2, never a traceback, and nothing written."""
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *args]) == 2
    captured = capsys.readouterr()
    assert (captured.out, captured.err) == ("", message)
    assert not out.exists()


def test_the_default_host_is_launchd(tmp_path):
    """CI runs on Linux, so a default read from the platform would switch every render."""
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    assert (out / "com.marketlake.daemon.plist").exists()


def test_the_launchd_group_still_defaults_to_staff(tmp_path):
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    assert "<string>staff</string>" in (out / "com.marketlake.daemon.plist").read_text()


# ``\x1b`` is a control character that ``\s`` does not match, unlike ``\x1c`` to ``\x1f``.
@pytest.mark.parametrize("character", ["%", "$", "'", '"', "\\", " ", "\t", "\x01", "\x1b"])
@pytest.mark.parametrize("flag", ["--home", "--lake-mount", "--config"])
def test_a_character_systemd_would_expand_is_refused_with_one_line(
    flag, character, tmp_path, capsys
):
    args = list(SYSTEMD_RENDER_ARGS)
    value = f"/srv/a{character}b"
    if flag in args:
        args[args.index(flag) + 1] = value
    else:
        args += [flag, value]
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *args]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith(f"render: {flag} {value!r} ")
    assert captured.err.count("\n") == 1
    assert not out.exists()


def test_an_empty_owner_is_refused_with_one_line(tmp_path, capsys):
    """An empty ``User=`` would leave systemd to run every job as root."""
    args = list(SYSTEMD_RENDER_ARGS)
    args[args.index("--owner") + 1] = ""
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *args]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("render: --owner '' is empty ")
    assert captured.err.count("\n") == 1
    assert not out.exists()


# -- the rendered files ----------------------------------------------------------------


def test_the_consumers_name_one_token_file(tmp_path):
    """The Sunday job reads the token, and the daemon resolves it from the ``HOME`` its
    unit sets. Both come from ``--home``, so the two cannot name different files."""
    out = tmp_path / "out"
    _render(out)
    token = "/home/someone/.config/marketlake/token.json"
    sunday = (out / "com.marketlake.sunday.service").read_text()
    (exec_start,) = [line for line in sunday.splitlines() if line.startswith("ExecStart=")]
    assert exec_start.split()[-2:] == ["--token", token]
    daemon = (out / "com.marketlake.daemon.service").read_text()
    assert "\nEnvironment=HOME=/home/someone\n" in daemon
    assert "--token" not in daemon
    # The vendor resolves the token through lake.paths.default_token_path. Binding that
    # to the shared rule is the daemon leg, and asserting the renderer's helper against
    # itself would prove nothing.
    assert str(default_token_path()) == str(config_dir() / TOKEN_FILE)
    assert cp.default_token_path("/home/someone") == token


def test_the_restart_offers_exactly_the_residents(tmp_path):
    out = tmp_path / "out"
    _render(out)
    script = (out / cp.RESTART_SCRIPT_FILE).read_text()
    for unit in RESIDENTS:
        assert f"UNITS=({unit})" in script, unit
    for label in (
        cp.SELF_CHECK_LABEL,
        cp.CALENDAR_PROBE_LABEL,
        cp.SUNDAY_LABEL,
        cp.STOP_LABEL,
        cp.VM_UP_LABEL,
    ):
        assert label not in script, label
    assert cp.EOD_SWEEP_LABEL not in script


def test_the_scripts_are_executable_and_nothing_else_is(tmp_path):
    out = tmp_path / "out"
    _render(out)
    scripts = {cp.INSTALL_SCRIPT_FILE, cp.UNINSTALL_SCRIPT_FILE, cp.RESTART_SCRIPT_FILE}
    for name in SYSTEMD_EXPECTED_FILES:
        mode = (out / name).stat().st_mode & 0o777
        assert mode == (0o755 if name in scripts else 0o644), name


def test_no_sudoers_reauth_wake_or_time_machine_step(tmp_path):
    """A VM has no pmset, no browser and no Time Machine, so none of them is rendered."""
    out = tmp_path / "out"
    _render(out)
    assert not (out / cp.SUDOERS_FILE).exists()
    assert not (out / cp.REAUTH_SCRIPT_FILE).exists()
    for path in out.iterdir():
        text = path.read_text()
        for word in ("pmset", "tmutil", "sudoers", "launchctl"):
            assert word not in text, (path.name, word)


def test_the_summary_counts_the_units_it_wrote(tmp_path, capsys):
    out = tmp_path / "out"
    _render(out)
    printed = capsys.readouterr().out
    assert f"  {len(UNITS)} unit files:" not in printed
    assert "  fourteen unit files:" in printed
    # The next step is the entry point, which holds the install lock, never a hand run
    # of the rendered install.sh, which would skip it.
    (next_line,) = [line for line in printed.splitlines() if line.startswith("  next: ")]
    assert next_line.startswith("  next: run deploy/linux-install.sh as root."), next_line
    assert cp.INSTALL_LOCK in next_line, next_line
    assert str(out.resolve() / cp.INSTALL_SCRIPT_FILE) not in printed


def test_a_seventh_job_reaches_both_renders(tmp_path, monkeypatch, capsys):
    """One roster, so a job added to ``all_jobs`` lands in the plists and the units both.

    The summary counts the grown roster too, which only a roster other than the real
    one can show: a count spelled as a literal fourteen reads right on the real roster.
    """
    original = cp.all_jobs

    def grown(host):
        seventh = host.job("com.marketlake.seventh", "lake.nothing", calendar=EVERY_DAY_AT_THREE)
        return (*original(host), seventh)

    monkeypatch.setattr(cp, "all_jobs", grown)
    launchd, systemd = tmp_path / "launchd", tmp_path / "systemd"
    assert cp.main(["render", "--out", str(launchd), *RENDER_ARGS]) == 0
    capsys.readouterr()
    _render(systemd)
    summary = capsys.readouterr().out.splitlines()
    assert "  sixteen unit files:" in summary, summary
    for name in ("com.marketlake.seventh.service", "com.marketlake.seventh.timer"):
        assert f"    {name}" in summary, name
    assert (launchd / "com.marketlake.seventh.plist").exists()
    timer = (systemd / "com.marketlake.seventh.timer").read_text()
    assert "\nOnCalendar=Mon..Sun 03:00:00 America/New_York\n" in timer
    assert (systemd / "com.marketlake.seventh.service").exists()
    install = (systemd / cp.INSTALL_SCRIPT_FILE).read_text()
    assert "com.marketlake.seventh.timer" in install
    uninstall = (systemd / cp.UNINSTALL_SCRIPT_FILE).read_text()
    assert "removes the sixteen units" in uninstall


# -- the stop and the morning ping (#868) -----------------------------------------------


def _lines(path: Path) -> list[str]:
    return path.read_text().splitlines()


def test_the_stop_runs_its_script_as_root_every_ten_minutes(tmp_path):
    out = tmp_path / "out"
    _render(out)
    service = _lines(out / "marketlake-stop.service")
    # No User= line, so systemd runs it as root, for the poweroff and the locks in /run.
    assert not [line for line in service if line.startswith(("User=", "Group="))], service
    for line in (
        "Type=oneshot",
        "ExecStart=/home/someone/marketlake/deploy/vm-stop.sh",
        "TimeoutStartSec=5min",
        "CPUSchedulingPolicy=idle",
        "IOSchedulingClass=idle",
        "RequiresMountsFor=/srv/lake",
    ):
        assert line in service, (line, service)
    # Only its timer starts it.
    assert "[Install]" not in service
    timer = _lines(out / "marketlake-stop.timer")
    assert "OnCalendar=*:0/10" in timer, timer
    assert not [line for line in timer if line.startswith("Persistent=")], timer
    assert "WantedBy=timers.target" in timer


def test_a_config_the_render_names_reaches_the_stop(tmp_path):
    out = tmp_path / "out"
    _render(out, "--config", "/srv/conf/config.yaml")
    service = _lines(out / "marketlake-stop.service")
    assert "Environment=MARKETLAKE_CONFIG=/srv/conf/config.yaml" in service, service
    # Without one the stop carries no environment of its own.
    bare = tmp_path / "bare"
    _render(bare)
    assert not [line for line in _lines(bare / "marketlake-stop.service") if "Environment" in line]


def test_the_morning_ping_runs_as_the_owner_at_0740_on_weekdays(tmp_path):
    out = tmp_path / "out"
    _render(out)
    service = _lines(out / "marketlake-up.service")
    assert "User=someone" in service, service
    (exec_start,) = [line for line in service if line.startswith("ExecStart=")]
    assert exec_start.endswith(" -m lake.control_plane ping vm-up"), exec_start
    assert "RequiresMountsFor=/srv/lake" in service
    timer = _lines(out / "marketlake-up.timer")
    assert "OnCalendar=Mon..Fri 07:40:00 America/New_York" in timer, timer
    # A VM that boots late must send nothing, so the check pages.
    assert not [line for line in timer if line.startswith("Persistent=")], timer


def test_the_systemd_only_units_stay_out_of_the_roster_and_the_refused_spans(tmp_path):
    """The units are rendered, and the hours a deploy may run do not move.

    ``deploy_window`` reads ``systemd_units``. A ten-minute timer there would refuse
    deploys all day, and the 07:40 ping would move the weekday span to 07:10.
    """
    out = tmp_path / "out"
    _render(out)
    power = {cp.STOP_LABEL, cp.VM_UP_LABEL}
    assert {unit.label for unit in cp.vm_power_units(_host())} == power
    for label in power:
        assert (out / f"{label}.service").exists(), label
        assert (out / f"{label}.timer").exists(), label
    roster = {unit.label for unit in cp.systemd_units(_host())}
    assert not roster & power
    assert not {unit.label for unit in dw.default_units()} & power
    assert not {job.label for job in cp.all_jobs(cp.LaunchdHost(*["/x"] * 5))} & power
    # 08:00 to 18:45 Eastern on a weekday, and 19:30 to 23:30 on a Sunday, as before.
    monday, sunday = date(2026, 10, 12), date(2026, 10, 11)
    assert dw.refused_span(monday, dw.default_units()) == dw.Span(
        datetime(2026, 10, 12, 8, 0, tzinfo=MARKET_TZ).astimezone(UTC),
        datetime(2026, 10, 12, 18, 45, tzinfo=MARKET_TZ).astimezone(UTC),
    )
    assert dw.refused_span(sunday, dw.default_units()) == dw.Span(
        datetime(2026, 10, 11, 19, 30, tzinfo=MARKET_TZ).astimezone(UTC),
        datetime(2026, 10, 11, 23, 30, tzinfo=MARKET_TZ).astimezone(UTC),
    )


# -- the harness -----------------------------------------------------------------------


class Harness:
    """A temporary install root, the fakes on ``PATH``, and a log of every call."""

    def __init__(self, tmp_path: Path, **env: str) -> None:
        self.tmp = tmp_path
        self.root = tmp_path / "root"
        self.root.mkdir(exist_ok=True)
        self.bin = tmp_path / "bin"
        install_fakes(self.bin)
        self.log = tmp_path / "log"
        self.log.write_text("")
        self.state = tmp_path / "state"
        self.state.mkdir(exist_ok=True)
        self.env = {
            "PATH": f"{self.bin}:/usr/bin:/bin",
            "LOG": str(self.log),
            "STATE": str(self.state),
            cp.INSTALL_ROOT_ENV: str(self.root),
            cp.INSTALL_TEST_ENV: "1",
            **env,
        }

    @property
    def unit_dir(self) -> Path:
        return self.root / cp.SYSTEMD_UNIT_DIR.lstrip("/")

    @property
    def dropin(self) -> Path:
        return self.root / cp.NEEDRESTART_DIR.lstrip("/") / cp.NEEDRESTART_INSTALLED

    @property
    def stamp_dir(self) -> Path:
        return self.root / cp.TIMER_STAMP_DIR.lstrip("/")

    def run(self, argv: list[str], **env: str) -> subprocess.CompletedProcess[str]:
        self.log.write_text("")
        return subprocess.run(
            argv, env={**self.env, **env}, capture_output=True, text=True, timeout=60
        )

    def calls(self) -> list[str]:
        return [line for line in self.log.read_text().splitlines() if line]

    def events(self) -> list[str]:
        path = self.state / "events"
        return path.read_text().splitlines() if path.exists() else []

    def pids(self) -> dict[str, str]:
        pid_dir = self.state / "pid"
        if not pid_dir.exists():
            return {}
        return {path.name: path.read_text() for path in pid_dir.iterdir()}

    def files(self) -> set[str]:
        return {
            str(path.relative_to(self.root))
            for path in self.root.rglob("*")
            if path.is_file() or path.is_symlink()
        }


@pytest.fixture
def rendered(tmp_path) -> Path:
    out = tmp_path / "out"
    _render(out)
    return out


def _install(harness: Harness, out: Path, **env: str) -> subprocess.CompletedProcess[str]:
    return harness.run([str(out / cp.INSTALL_SCRIPT_FILE)], **env)


def _changed(proc: subprocess.CompletedProcess[str]) -> list[str]:
    return [line for line in proc.stdout.splitlines() if line.startswith("changed: ")]


# -- install.sh ------------------------------------------------------------------------


def test_the_install_places_every_unit_and_the_dropin(tmp_path, rendered):
    harness = Harness(tmp_path)
    proc = _install(harness, rendered)
    assert proc.returncode == 0, proc.stderr
    for unit in UNITS:
        assert (harness.unit_dir / unit).read_bytes() == (rendered / unit).read_bytes(), unit
    assert harness.dropin.read_bytes() == (rendered / cp.NEEDRESTART_FILE).read_bytes()
    assert sorted(_changed(proc)) == sorted(
        f"changed: {name}" for name in [*UNITS, cp.NEEDRESTART_INSTALLED]
    )


def test_the_install_leaves_every_unit_and_the_dropin_at_0644(tmp_path, rendered):
    """The install sets the mode itself rather than carrying the render's.

    The rendered copies are made private first, so a copy that kept the source's mode
    would leave 0600 behind, as would an install at the wrong mode.
    """
    for name in [*UNITS, cp.NEEDRESTART_FILE]:
        (rendered / name).chmod(0o600)
    harness = Harness(tmp_path)
    proc = _install(harness, rendered)
    assert proc.returncode == 0, proc.stderr
    for path in [*(harness.unit_dir / unit for unit in UNITS), harness.dropin]:
        assert path.stat().st_mode & 0o777 == 0o644, path.name


def test_the_install_reloads_then_enables_only_what_has_an_install_section(tmp_path, rendered):
    harness = Harness(tmp_path)
    proc = _install(harness, rendered)
    assert proc.returncode == 0, proc.stderr
    calls = harness.calls()
    reload = calls.index("systemctl daemon-reload")
    enabled = [i for i, line in enumerate(calls) if line.startswith("systemctl enable")]
    assert [calls[i] for i in enabled] == [
        f"systemctl enable --now {unit}" for unit in [*RESIDENTS, *sorted(TIMERS, key=_order)]
    ]
    assert reload < min(enabled)
    # Both residents run, and no timer-run service was started by the install.
    assert sorted(harness.pids()) == sorted([*RESIDENTS, *TIMERS])


def _order(timer: str) -> int:
    """The roster's order, then the systemd-only units', which is the order the install
    enables the timers in."""
    labels = [job.label for job in (*cp.all_jobs(_host()), *cp.vm_power_units(_host()))]
    return labels.index(timer.removesuffix(".timer"))


def _host() -> cp.SystemdHost:
    pairs = dict(zip(SYSTEMD_RENDER_ARGS[2::2], SYSTEMD_RENDER_ARGS[3::2], strict=True))
    return cp.SystemdHost(
        python=pairs["--python"],
        owner=pairs["--owner"],
        home=pairs["--home"],
        project_dir=pairs["--project-dir"],
        lake_mount=pairs["--lake-mount"],
    )


def test_a_second_install_changes_nothing_and_restarts_nothing(tmp_path, rendered):
    harness = Harness(tmp_path)
    assert _install(harness, rendered).returncode == 0
    before = harness.pids()
    starts = harness.events()
    proc = _install(harness, rendered)
    assert proc.returncode == 0, proc.stderr
    assert _changed(proc) == []
    assert harness.pids() == before
    # The second run started and restarted nothing at all.
    assert harness.events() == starts
    assert not [line for line in harness.calls() if "restart" in line or " stop " in line]


def test_a_changed_unit_is_reported_and_copied_but_not_restarted(tmp_path, rendered):
    harness = Harness(tmp_path)
    assert _install(harness, rendered).returncode == 0
    before = harness.pids()
    daemon = rendered / "com.marketlake.daemon.service"
    daemon.write_text(daemon.read_text().replace("RestartSec=10", "RestartSec=11"))
    proc = _install(harness, rendered)
    assert proc.returncode == 0, proc.stderr
    assert _changed(proc) == ["changed: com.marketlake.daemon.service"]
    assert "RestartSec=11" in (harness.unit_dir / "com.marketlake.daemon.service").read_text()
    assert harness.pids() == before


def test_a_unit_dropped_from_the_render_is_retired_with_its_stamp(tmp_path, rendered):
    harness = Harness(tmp_path)
    harness.unit_dir.mkdir(parents=True)
    harness.stamp_dir.mkdir(parents=True)
    # A com.marketlake unit and a systemd-only one the render no longer names, such as a
    # stop unit a later render dropped, which would otherwise keep powering the VM off.
    retired = (
        "com.marketlake.retired.service",
        "com.marketlake.retired.timer",
        "marketlake-retired.service",
        "marketlake-retired.timer",
    )
    for name in retired:
        (harness.unit_dir / name).write_text("[Unit]\n")
    (harness.stamp_dir / "stamp-com.marketlake.retired.timer").write_text("")
    (harness.stamp_dir / "stamp-marketlake-retired.timer").write_text("")
    # A systemctl edit drop-in directory, and a unit that is not marketlake's.
    edit = harness.unit_dir / "com.marketlake.daemon.service.d"
    edit.mkdir()
    (edit / "override.conf").write_text("[Service]\n")
    (harness.unit_dir / "other.service").write_text("[Unit]\n")

    proc = _install(harness, rendered)
    assert proc.returncode == 0, proc.stderr
    for name in retired:
        assert not (harness.unit_dir / name).exists(), name
    assert not (harness.stamp_dir / "stamp-com.marketlake.retired.timer").exists()
    assert not (harness.stamp_dir / "stamp-marketlake-retired.timer").exists()
    assert (edit / "override.conf").exists()
    assert (harness.unit_dir / "other.service").exists()
    calls = harness.calls()
    for name in retired:
        stop = calls.index(f"systemctl stop {name}")
        disable = calls.index(f"systemctl disable {name}")
        assert stop < disable < calls.index("systemctl daemon-reload"), calls


def test_the_dropin_directory_is_created_when_needrestart_is_absent(tmp_path, rendered):
    harness = Harness(tmp_path)
    assert not harness.dropin.parent.exists()
    assert _install(harness, rendered).returncode == 0
    assert harness.dropin.exists()


def test_the_install_reads_back_a_resident_that_cannot_start_and_still_succeeds(tmp_path, rendered):
    """``enable --now`` exits 0 whatever the start did, so the read-back is the signal."""
    harness = Harness(tmp_path)
    proc = _install(harness, rendered, FAIL_START="com.marketlake.daemon.service")
    assert proc.returncode == 0, proc.stderr
    assert "  ExecMainStatus=203" in proc.stdout, proc.stdout
    assert "  Result=exit-code" in proc.stdout, proc.stdout
    for prop in ("ActiveState", "SubState", "NRestarts", "Result", "ExecMainStatus"):
        assert proc.stdout.count(f"  {prop}=") == len(RESIDENTS), prop


def test_a_read_back_that_fails_never_fails_the_install(tmp_path, rendered):
    """The units are already placed and started by then, so a failed read is no reason
    to exit nonzero, and the next resident is still read."""
    failing, other = RESIDENTS
    harness = Harness(tmp_path)
    proc = _install(harness, rendered, FAIL_SHOW=failing)
    assert proc.returncode == 0, proc.stderr
    assert "Failed to get properties" in proc.stderr
    lines = proc.stdout.splitlines()
    shows = [line for line in lines if line.startswith("+ systemctl show")]
    assert [line.rsplit(" ", 1)[-1] for line in shows] == [failing, other], shows
    # The other resident's read-back follows its own echo, every property once.
    after = lines[lines.index(shows[-1]) + 1 :]
    for prop in ("ActiveState", "SubState", "NRestarts", "Result", "ExecMainStatus"):
        assert [line for line in lines if line.startswith(f"  {prop}=")] == [
            line for line in after if line.startswith(f"  {prop}=")
        ], prop
        assert sum(line.startswith(f"  {prop}=") for line in after) == 1, prop


def test_the_install_echoes_each_command_before_running_it(tmp_path, rendered):
    harness = Harness(tmp_path)
    proc = _install(harness, rendered)
    assert proc.returncode == 0, proc.stderr
    echoed = [line.removeprefix("+ ") for line in proc.stdout.splitlines() if line[:2] == "+ "]
    for call in harness.calls():
        if call.startswith("systemctl ") and not call.startswith("systemctl show"):
            assert call in echoed, call


@pytest.mark.parametrize("script", [cp.INSTALL_SCRIPT_FILE, cp.UNINSTALL_SCRIPT_FILE])
def test_the_scripts_refuse_without_root(script, tmp_path, rendered):
    harness = Harness(tmp_path)
    proc = harness.run([str(rendered / script)], FAKE_UID="1000")
    assert proc.returncode == 2
    assert proc.stderr == f"{script}: run this as root, for example with sudo\n"
    assert not [line for line in harness.calls() if line.startswith("systemctl")]
    assert harness.files() == set()


@pytest.mark.parametrize("script", [cp.INSTALL_SCRIPT_FILE, cp.UNINSTALL_SCRIPT_FILE])
def test_the_scripts_refuse_an_install_root_outside_a_test(script, tmp_path, rendered):
    """A prefix leaked from a caller would exit 0 while the real units stayed old."""
    harness = Harness(tmp_path)
    proc = harness.run([str(rendered / script)], **{cp.INSTALL_TEST_ENV: ""})
    assert proc.returncode == 2
    assert f"{cp.INSTALL_ROOT_ENV} is {harness.root}, which only a test may set" in proc.stderr
    assert not [line for line in harness.calls() if line.startswith("systemctl")]
    assert harness.files() == set()


def test_the_banner_names_the_install_root(tmp_path, rendered):
    harness = Harness(tmp_path)
    proc = _install(harness, rendered)
    assert proc.stdout.splitlines()[0] == f"{cp.INSTALL_SCRIPT_FILE}: install root {harness.root}"


# Every item of a bash array, in the form an empty array survives under ``set -u``.
GUARDED_EXPANSION = re.compile(r'\$\{(\w+)\[@\]\+"\$\{\1\[@\]\}"\}')


def test_an_empty_list_is_harmless_in_both_scripts(tmp_path, monkeypatch, rendered):
    """bash 3.2 reads an empty array as unbound under ``set -u``, so each read is guarded.

    The run with no units fails on a Mac's ``/bin/bash`` when a loop is bare. A current
    bash, as on CI's Linux runner, accepts the bare form, so the text check on the real
    render is what fails there.
    """
    for script in (cp.INSTALL_SCRIPT_FILE, cp.UNINSTALL_SCRIPT_FILE):
        bare = GUARDED_EXPANSION.sub("", (rendered / script).read_text())
        assert "[@]" not in bare, script

    monkeypatch.setattr(cp, "systemd_units", lambda host: ())
    monkeypatch.setattr(cp, "vm_power_units", lambda host: ())
    empty = tmp_path / "empty"
    empty.mkdir()
    (empty / cp.NEEDRESTART_FILE).write_text(cp.needrestart_dropin())
    for name, text in (
        (cp.INSTALL_SCRIPT_FILE, cp.systemd_install_script(_host())),
        (cp.UNINSTALL_SCRIPT_FILE, cp.systemd_uninstall_script(_host())),
    ):
        assert "UNITS=()" in text, name
        (empty / name).write_text(text)
        (empty / name).chmod(0o755)
    harness = Harness(tmp_path)
    installed = _install(harness, empty)
    assert installed.returncode == 0, installed.stderr
    removed = _uninstall(harness, empty)
    assert removed.returncode == 0, removed.stderr


# -- uninstall.sh ----------------------------------------------------------------------


def _uninstall(harness: Harness, out: Path) -> subprocess.CompletedProcess[str]:
    return harness.run([str(out / cp.UNINSTALL_SCRIPT_FILE)])


def test_the_uninstall_removes_exactly_what_the_install_placed(tmp_path, rendered):
    harness = Harness(tmp_path)
    harness.unit_dir.mkdir(parents=True)
    (harness.unit_dir / "other.service").write_text("[Unit]\n")
    before = harness.files()
    assert _install(harness, rendered).returncode == 0
    placed = harness.files() - before
    assert placed == {
        *(f"{cp.SYSTEMD_UNIT_DIR.lstrip('/')}/{unit}" for unit in UNITS),
        f"{cp.NEEDRESTART_DIR.lstrip('/')}/{cp.NEEDRESTART_INSTALLED}",
    }
    # The persistent timers have fired, so their stamps exist.
    harness.stamp_dir.mkdir(parents=True)
    for stamp in PERSISTENT_STAMPS:
        (harness.stamp_dir / stamp).write_text("")
    proc = _uninstall(harness, rendered)
    assert proc.returncode == 0, proc.stderr
    assert harness.files() == before
    assert harness.pids() == {}


def test_the_uninstall_disables_before_it_removes_and_reloads_last(tmp_path, rendered):
    harness = Harness(tmp_path)
    assert _install(harness, rendered).returncode == 0
    proc = _uninstall(harness, rendered)
    assert proc.returncode == 0, proc.stderr
    calls = harness.calls()
    disables = [i for i, line in enumerate(calls) if line.startswith("systemctl disable --now")]
    assert len(disables) == len(UNITS), calls
    assert calls[-1] == "systemctl daemon-reload"
    # Timers go first, so none fires a service during the teardown.
    first_service = min(i for i in disables if calls[i].endswith(".service"))
    assert max(i for i in disables if calls[i].endswith(".timer")) < first_service


def test_the_uninstall_converges_from_a_partial_install(tmp_path, rendered):
    """systemd 255 refuses ``disable`` on a missing file, so an absent unit is skipped."""
    harness = Harness(tmp_path)
    assert _install(harness, rendered).returncode == 0
    for unit in ("com.marketlake.sunday.timer", "com.marketlake.dashboard.service"):
        (harness.unit_dir / unit).unlink()
    harness.dropin.unlink()
    proc = _uninstall(harness, rendered)
    assert proc.returncode == 0, proc.stderr
    assert "com.marketlake.sunday.timer is not installed, nothing to disable" in proc.stdout
    assert not list(harness.unit_dir.iterdir())
    # And a second run, with nothing installed at all, finishes too.
    again = _uninstall(harness, rendered)
    assert again.returncode == 0, again.stderr


def test_the_uninstall_warns_that_the_checks_go_silent_on_a_primary(tmp_path, rendered):
    harness = Harness(tmp_path)
    proc = _uninstall(harness, rendered)
    assert proc.returncode == 0, proc.stderr
    last = proc.stdout.splitlines()[-1]
    assert "primary" in last and "dead-man checks" in last, last
    header = (rendered / cp.UNINSTALL_SCRIPT_FILE).read_text().split("set -euo pipefail")[0]
    # The systemd-only checks go silent too, since the uninstall removes the stop and the
    # morning ping with the rest.
    for slug in (*cp.live_check_slugs(), *cp.vm_power_slugs()):
        assert slug in header, slug
    assert "nine dead-man checks go silent" in header, header


# -- restart.sh ------------------------------------------------------------------------


def _seed_installed(harness: Harness, rendered: Path) -> None:
    """The state a finished install leaves, written directly rather than by install.sh.

    Every unit file is in place and was read at the last reload, and the two residents
    run as pids 1001 and 1002. Writing it costs nothing, where running the install first
    costs a few hundred process starts per test. The install's own tests cover reaching
    this state.
    """
    harness.unit_dir.mkdir(parents=True, exist_ok=True)
    reloaded = harness.state / "reloaded"
    reloaded.mkdir(exist_ok=True)
    for unit in UNITS:
        shutil.copy2(rendered / unit, harness.unit_dir / unit)
        shutil.copy2(rendered / unit, reloaded / unit)
    pids = harness.state / "pid"
    pids.mkdir(exist_ok=True)
    for number, unit in enumerate(RESIDENTS, start=1001):
        (pids / unit).write_text(str(number))
    (harness.state / "pidseq").write_text("1002")


def _restart(
    tmp_path: Path, rendered: Path, *argv: str, **env: str
) -> tuple[subprocess.CompletedProcess[str], Harness]:
    """Run restart.sh against the fakes, on a host the install has finished."""
    harness = Harness(tmp_path)
    _seed_installed(harness, rendered)
    proc = harness.run([str(rendered / cp.RESTART_SCRIPT_FILE), *argv], **env)
    return proc, harness


def _restarts(harness: Harness) -> list[str]:
    return [line for line in harness.calls() if line.startswith("systemctl restart")]


def test_the_restart_defaults_to_the_dashboard(tmp_path, rendered):
    proc, harness = _restart(tmp_path, rendered)
    assert proc.returncode == 0, proc.stderr
    assert _restarts(harness) == ["systemctl restart com.marketlake.dashboard.service"]


def test_the_restart_takes_the_daemon_only_when_named(tmp_path, rendered):
    proc, harness = _restart(tmp_path, rendered, "daemon")
    assert proc.returncode == 0, proc.stderr
    assert _restarts(harness) == ["systemctl restart com.marketlake.daemon.service"]
    proc, harness = _restart(tmp_path, rendered, "all")
    assert proc.returncode == 0, proc.stderr
    assert _restarts(harness) == [f"systemctl restart {unit}" for unit in RESIDENTS]


def test_the_restart_proves_the_process_changed(tmp_path, rendered):
    proc, harness = _restart(tmp_path, rendered)
    assert proc.returncode == 0, proc.stderr
    after = harness.pids()["com.marketlake.dashboard.service"]
    assert f"com.marketlake.dashboard.service restarted: pid 1002 -> {after}," in proc.stdout
    assert after != "1002"


def test_the_restart_waits_for_a_relaunch_that_is_not_instant(tmp_path, rendered):
    proc, harness = _restart(tmp_path, rendered, RESTART_DELAY="3")
    assert proc.returncode == 0, proc.stderr + proc.stdout
    after = harness.pids()["com.marketlake.dashboard.service"]
    assert f"restarted: pid 1002 -> {after}," in proc.stdout, proc.stdout


def test_the_restart_runs_git_as_the_owner_and_warns_about_the_tree(tmp_path, rendered):
    """git refuses a checkout another account owns, so under root it must run as the owner."""
    proc, harness = _restart(tmp_path, rendered, BRANCH="claude/x", DIRTY="1")
    assert proc.returncode == 0, proc.stderr
    assert "not on main" in proc.stdout and "uncommitted changes" in proc.stdout
    calls = harness.calls()
    gits = [i for i, line in enumerate(calls) if line.startswith("git ")]
    assert gits, calls
    for i in gits:
        assert calls[i - 1] == f"sudo -u someone -H {calls[i]}", calls[i - 1 : i + 1]
    clean, _ = _restart(tmp_path, rendered)
    assert "WARNING" not in clean.stdout, clean.stdout


def test_the_restart_survives_a_project_dir_that_is_not_a_checkout(tmp_path, rendered):
    proc, _ = _restart(tmp_path, rendered, IS_REPO="0")
    assert proc.returncode == 0, proc.stderr
    assert "not a git checkout" in proc.stdout


def test_the_restart_exits_1_for_a_unit_that_is_not_loaded(tmp_path, rendered):
    harness = Harness(tmp_path)
    proc = harness.run([str(rendered / cp.RESTART_SCRIPT_FILE)])
    assert proc.returncode == 1
    assert "com.marketlake.dashboard.service is not loaded. Run the install first." in proc.stderr
    assert _restarts(harness) == []


def test_the_restart_exits_1_while_a_unit_needs_a_reload(tmp_path, rendered):
    """A restart between a unit's copy and the reload would run the old definition."""
    harness = Harness(tmp_path)
    _seed_installed(harness, rendered)
    unit = harness.unit_dir / "com.marketlake.dashboard.service"
    unit.write_text(unit.read_text() + "# changed\n")
    proc = harness.run([str(rendered / cp.RESTART_SCRIPT_FILE)])
    assert proc.returncode == 1
    assert "Run the install first." in proc.stderr
    assert "old definition" in proc.stderr
    assert _restarts(harness) == []


def test_the_restart_exits_1_when_systemctl_restart_fails(tmp_path, rendered):
    """Under ``Type=exec`` the restart itself fails when the interpreter cannot start."""
    proc, _ = _restart(tmp_path, rendered, FAIL_START="com.marketlake.dashboard.service")
    assert proc.returncode == 1
    assert "will not stay up. systemctl restart failed." in proc.stderr
    assert "journalctl -u com.marketlake.dashboard.service" in proc.stderr


def test_the_restart_exits_1_for_a_unit_that_will_not_stay_up(tmp_path, rendered):
    proc, _ = _restart(tmp_path, rendered, RESTART_MODE="crash")
    assert proc.returncode == 1
    assert "will not stay up. pid went" in proc.stderr
    assert "journalctl -u com.marketlake.dashboard.service" in proc.stderr


def test_the_restart_exits_1_for_a_unit_that_never_comes_back(tmp_path, rendered):
    proc, _ = _restart(tmp_path, rendered, RESTART_MODE="never")
    assert proc.returncode == 1
    assert "has no pid after the restart" in proc.stderr


def test_the_restart_exits_1_when_the_pid_did_not_change(tmp_path, rendered):
    proc, _ = _restart(tmp_path, rendered, RESTART_MODE="same")
    assert proc.returncode == 1
    assert "did not restart" in proc.stderr


def test_the_restart_exits_2_on_usage(tmp_path, rendered):
    proc, harness = _restart(tmp_path, rendered, "dashbaord")
    assert proc.returncode == 2
    assert proc.stderr == "usage: ./restart.sh [daemon|dashboard|all]\n"
    assert _restarts(harness) == []


def test_the_restart_exits_2_without_root(tmp_path, rendered):
    proc, harness = _restart(tmp_path, rendered, FAKE_UID="1000")
    assert proc.returncode == 2
    assert proc.stderr == "restart.sh: run this as root, for example with sudo\n"
    assert _restarts(harness) == []


# -- the install entry point -----------------------------------------------------------


OWNER = "someone"


def _checkout(tmp_path: Path) -> tuple[Path, Path]:
    """A checkout holding the entry point and a venv interpreter, and the owner's home.

    The entry point is a link to the repository's own file, which finds its checkout from
    the path it was run by, so it runs against this one. A write through that link
    rewrites the tracked file, so a test that needs a changed script must unlink it and
    write its own. The interpreter is a wrapper
    around this test's own, so the real render runs. The owner's ``uv`` is a fake that
    records the call and where it ran. Both are links to ``tests.support.fake_bin``'s one
    program, so none of the three is a new file a Mac scans on its first run.
    """
    checkout = tmp_path / "checkout"
    (checkout / "deploy").mkdir(parents=True)
    (checkout / "deploy" / ENTRY_POINT.name).symlink_to(ENTRY_POINT)
    install(checkout / ".venv" / "bin" / "python", f'#!/bin/bash\nexec {sys.executable} "$@"\n')
    home = tmp_path / "home"
    install(
        home / ".local" / "bin" / "uv",
        '#!/bin/bash\nprintf \'uv %s in %s\\n\' "$*" "$PWD" >> "$LOG"\nexit "${UV_RC:-0}"\n',
    )
    return checkout, home


def _entry(tmp_path: Path, *argv: str, **env: str):
    checkout, home = _checkout(tmp_path)
    harness = Harness(tmp_path, FAKE_OWNER=OWNER, FAKE_HOME=str(home))
    proc = harness.run([str(checkout / "deploy" / ENTRY_POINT.name), *argv], **env)
    return proc, harness, checkout, home


def test_the_entry_point_is_tracked_executable():
    assert os.access(ENTRY_POINT, os.X_OK)


@pytest.mark.parametrize(
    ("argv", "env", "message"),
    [
        (
            ["--owner", OWNER, "--lake-mount", "/srv/lake"],
            {"FAKE_UID": "1000"},
            "linux-install: run this as root, for example with sudo\n",
        ),
        (
            ["--lake-mount", "/srv/lake"],
            {},
            "linux-install: --owner is required: the account the jobs run as\n",
        ),
        (
            ["--owner", OWNER],
            {},
            "linux-install: --lake-mount is required: the lake's mount point, or lake_root"
            " when the lake sits on the root volume\n",
        ),
        (
            ["--owner", "nobody-here", "--lake-mount", "/srv/lake"],
            {},
            "linux-install: --owner nobody-here names no account on this host\n",
        ),
        (
            ["--owner", "1000", "--lake-mount", "/srv/lake"],
            {},
            f"linux-install: --owner 1000 is the uid of {OWNER}. Give the account name\n",
        ),
        (
            ["--owner", OWNER, "--lake-mount", "/srv/lake", "--bogus"],
            {},
            "linux-install: unknown argument --bogus. Usage: linux-install.sh --owner"
            " <account> --lake-mount <path> [--config <path>] [--sync-only]."
            " Only vm-bootstrap.sh passes --sync-only\n",
        ),
        (
            ["--owner", OWNER, "--lake-mount"],
            {},
            "linux-install: --lake-mount needs a value\n",
        ),
        (
            ["--owner", OWNER, "--lake-mount", "/srv/lake"],
            {cp.INSTALL_TEST_ENV: ""},
            "",
        ),
        (
            ["--owner", OWNER, "--lake-mount", "/srv/lake"],
            {"FLOCK_RC": "1"},
            "",
        ),
    ],
    ids=[
        "not root",
        "no owner",
        "no lake mount",
        "no such account",
        "a uid for the owner",
        "unknown flag",
        "flag without value",
        "leaked install root",
        "lock wait ran out",
    ],
)
def test_the_entry_point_refuses_before_it_renders(argv, env, message, tmp_path):
    proc, harness, _, home = _entry(tmp_path, *argv, **env)
    assert proc.returncode == 2, proc.stdout + proc.stderr
    if message:
        assert proc.stderr == message
    else:
        assert proc.stderr.startswith("linux-install: "), proc.stderr
        assert proc.stderr.count("\n") == 1, proc.stderr
    calls = harness.calls()
    assert not [line for line in calls if line.startswith(("uv ", "systemctl "))], calls
    assert not (home / ".local" / "state").exists()


@pytest.mark.parametrize(
    "argv",
    [
        # Last, so a parser that wanted a value after the flag would refuse it, and first,
        # so one that took the next argument as its value would leave --owner unread.
        ["--owner", OWNER, "--lake-mount", "/srv/lake", "--sync-only"],
        ["--sync-only", "--owner", OWNER, "--lake-mount", "/srv/lake"],
    ],
    ids=["flag last", "flag first"],
)
def test_sync_only_syncs_under_the_lock_and_installs_nothing(argv, tmp_path):
    """The bootstrap's first call: the venv the config steps run from, and no unit.

    The units start only from a run without the flag, after config.yaml exists.
    """
    proc, harness, checkout, home = _entry(tmp_path, *argv)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    calls = harness.calls()
    sync = f"uv sync --frozen --no-dev in {checkout}"
    assert "flock -w 600 9" in calls
    assert sync in calls
    assert calls.index("flock -w 600 9") < calls.index(sync)
    # The sync runs as the owner, and nothing else goes through sudo.
    assert _sudo_calls(harness) == [
        f"sudo -u {OWNER} -H {home}/.local/bin/uv sync --frozen --no-dev"
    ]
    assert not (home / ".local" / "state").exists()
    assert not [line for line in calls if line.startswith("systemctl")], calls
    assert not harness.unit_dir.exists()
    assert proc.stdout.splitlines()[-1] == "linux-install: done"


@pytest.mark.parametrize("flag", [[], ["--sync-only"]], ids=["install", "sync only"])
def test_a_failed_sync_exits_1_and_goes_no_further(flag, tmp_path):
    """The bootstrap retries exit 1 as a refused proxy during uv sync, so it must see it."""
    proc, harness, _, home = _entry(
        tmp_path, "--owner", OWNER, "--lake-mount", "/srv/lake", *flag, UV_RC="1"
    )
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "linux-install: done" not in proc.stdout.splitlines()
    assert not (home / ".local" / "state").exists()
    assert not [line for line in harness.calls() if line.startswith("systemctl")]


@pytest.mark.parametrize(
    ("argv", "env", "message"),
    [
        (
            ["--owner", OWNER, "--lake-mount", "/srv/lake"],
            {"FAKE_UID": "1000"},
            "linux-install: run this as root, for example with sudo\n",
        ),
        (
            ["--owner", OWNER],
            {},
            "linux-install: --lake-mount is required: the lake's mount point, or lake_root"
            " when the lake sits on the root volume\n",
        ),
        (
            ["--owner", "1000", "--lake-mount", "/srv/lake"],
            {},
            f"linux-install: --owner 1000 is the uid of {OWNER}. Give the account name\n",
        ),
    ],
    ids=["not root", "no lake mount", "a uid for the owner"],
)
def test_sync_only_refuses_what_a_full_install_refuses(argv, env, message, tmp_path):
    proc, harness, _, _ = _entry(tmp_path, *argv, "--sync-only", **env)
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert proc.stderr == message
    assert not [line for line in harness.calls() if line.startswith("uv ")]


def test_the_lock_refusal_names_the_wait(tmp_path):
    proc, harness, _, _ = _entry(
        tmp_path, "--owner", OWNER, "--lake-mount", "/srv/lake", FLOCK_RC="1"
    )
    lock = f"{harness.root}{cp.INSTALL_LOCK}"
    assert proc.stderr == (
        f"linux-install: another install held {lock} for 600 seconds, so this one gave up\n"
    )


def test_the_entry_point_finds_its_checkout_under_an_exported_cdpath(tmp_path):
    """A relative invocation with ``CDPATH`` exported still resolves this checkout.

    ``cd`` searches ``CDPATH`` for a relative directory and prints the one it chose. The
    decoy holds a ``deploy`` directory of its own, so an unguarded ``cd`` would both print
    into the checkout path and pick the wrong directory.
    """
    checkout, home = _checkout(tmp_path)
    decoy = tmp_path / "decoy"
    (decoy / "deploy").mkdir(parents=True)
    harness = Harness(tmp_path, FAKE_OWNER=OWNER, FAKE_HOME=str(home))
    proc = subprocess.run(
        [f"deploy/{ENTRY_POINT.name}", "--owner", OWNER, "--lake-mount", "/srv/lake"],
        cwd=checkout,
        env={**harness.env, "CDPATH": str(decoy)},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert f"uv sync --frozen --no-dev in {checkout}" in harness.calls()
    daemon = (harness.unit_dir / "com.marketlake.daemon.service").read_text()
    assert f"\nWorkingDirectory={checkout}\n" in daemon


def _as_owner(checkout: Path, home: Path, *flags: str, swapped: bool) -> list[str]:
    """Every step one entry-point run should send through ``sudo -u <owner> -H``, in order.

    Each one writes under the owner's home or the checkout, so each runs as the owner, and
    no other step goes through sudo. ``swapped`` is a run that finds a live render to
    move aside.
    """
    python = checkout / ".venv" / "bin" / "python"
    state = home / ".local" / "state" / "marketlake"
    live, new, old = state / "systemd", state / "systemd.new", state / "systemd.old"
    steps = [
        f"{home}/.local/bin/uv sync --frozen --no-dev",
        f"rm -rf {new} {old}",
        f"mkdir -p {state}",
        " ".join(
            [
                f"{python} -m lake.control_plane render --init systemd --out {new}",
                f"--python {python} --project-dir {checkout} --home {home} --owner {OWNER}",
                *flags,
            ]
        ),
        *([f"mv {live} {old}"] if swapped else []),
        f"mv {new} {live}",
        f"rm -rf {old}",
    ]
    return [f"sudo -u {OWNER} -H {step}" for step in steps]


def _sudo_calls(harness: Harness) -> list[str]:
    return [line for line in harness.calls() if line.startswith("sudo ")]


def test_two_runs_swap_the_render_and_carry_the_lake_mount(tmp_path):
    """The real render, twice, with the units installed by the rendered install.sh.

    A re-run swaps the render directory rather than nesting it, a leftover from a killed
    run is cleared first, and ``--lake-mount`` reaches every service. The first run
    passes ``--config`` and the second does not, so the optional flag's empty form is
    covered under bash 3.2's ``set -u`` too.
    """
    checkout, home = _checkout(tmp_path)
    harness = Harness(tmp_path, FAKE_OWNER=OWNER, FAKE_HOME=str(home))
    entry = str(checkout / "deploy" / ENTRY_POINT.name)
    state = home / ".local" / "state" / "marketlake"
    live = state / "systemd"

    first = harness.run(
        [entry, "--owner", OWNER, "--lake-mount", "/srv/lake", "--config", "/srv/ml.yaml"]
    )
    assert first.returncode == 0, first.stdout + first.stderr
    calls = harness.calls()
    # The lock first, then uv by absolute path as the owner, in the checkout.
    assert "flock -w 600 9" in calls
    assert f"uv sync --frozen --no-dev in {checkout}" in calls
    assert calls.index("flock -w 600 9") < calls.index(f"uv sync --frozen --no-dev in {checkout}")
    # Every write under the owner's home or the checkout runs as the owner, and only those.
    assert _sudo_calls(harness) == _as_owner(
        checkout,
        home,
        "--lake-mount /srv/lake --config /srv/ml.yaml",
        swapped=False,
    )
    for unit in UNITS:
        if unit.endswith(".service"):
            text = (harness.unit_dir / unit).read_text()
            assert "\nRequiresMountsFor=/srv/lake\n" in text, unit
            assert "\nEnvironment=MARKETLAKE_CONFIG=/srv/ml.yaml\n" in text, unit
            if unit == STOP_SERVICE:
                assert f"\nExecStart={checkout}/deploy/vm-stop.sh\n" in text, unit
                continue
            assert f"\nWorkingDirectory={checkout}\n" in text, unit
            assert f"\nExecStart={checkout}/.venv/bin/python -m " in text, unit
    assert sorted(path.name for path in state.iterdir()) == ["systemd"]
    banners = [line for line in first.stdout.splitlines() if line.startswith("linux-install: ")]
    assert banners[0] == f"linux-install: install root {harness.root}"
    assert banners[-1] == "linux-install: done"

    # A killed run's leftovers, one holding a file its render had written, and a file
    # the render no longer names.
    (state / "systemd.new").mkdir()
    (state / "systemd.new" / "half-written.service").write_text("")
    (state / "systemd.old").mkdir()
    (live / "stale.txt").write_text("")
    second = harness.run([entry, "--owner", OWNER, "--lake-mount", "/srv/other"])
    assert second.returncode == 0, second.stdout + second.stderr
    assert _sudo_calls(harness) == _as_owner(
        checkout, home, "--lake-mount /srv/other", swapped=True
    )
    assert sorted(path.name for path in state.iterdir()) == ["systemd"]
    assert sorted(path.name for path in live.iterdir()) == sorted(SYSTEMD_EXPECTED_FILES)
    for unit in UNITS:
        if unit.endswith(".service"):
            text = (harness.unit_dir / unit).read_text()
            assert "\nRequiresMountsFor=/srv/other\n" in text, unit
            assert "MARKETLAKE_CONFIG" not in text, unit
    # The second install copied the changed units and restarted nothing.
    assert "changed: com.marketlake.daemon.service" in second.stdout
    assert not [line for line in harness.calls() if line.startswith("systemctl restart")]
