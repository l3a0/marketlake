"""``deploy/vm-stop.sh``, the hosted VM's stop, run for real against fakes (#868).

The script runs here under ``/bin/bash`` with ``MARKETLAKE_INSTALL_ROOT`` pointed at a
temporary directory, from ``/`` as systemd runs a unit with no working directory. Every
tool it calls is a fake from ``tests.support.fake_systemd`` or ``tests.support.fake_disk``:
``systemctl``, which keeps each unit's state in a directory and records ``poweroff``,
``loginctl``, which prints the sessions a test gives it in systemd 255's columns, ``flock``,
whose descriptor form answers one exit code per call, ``sudo``, which clears the
environment as the real one does, and the checkout's venv ``python``, which answers
``lake.deploy_window`` and ``lake.control_plane ping``.

``PATH`` holds the fakes, then a directory of links to the few real tools the scripts
call. ``/usr/bin`` is never on it, so a Linux runner's own ``systemctl`` or ``loginctl``
cannot answer for a fake. The checkout's ``deploy/vm-stop.sh`` and ``deploy/busy-check.sh``
are links to the tracked files, so the script finds the busy check beside itself as it
does on the VM.

Each case starts from a VM where every check passes: the switch exists, the VM has been
up two hours, no one is logged in, nothing is busy, the window allows a stop and no
deploy runs. A test changes the one thing it is about.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from lake import control_plane as cp
from tests.support.fake_bin import checked_links, dispatcher, install
from tests.support.fake_disk import FAKE_VENV_PYTHON, install_disk_fakes

REPO_ROOT = Path(__file__).resolve().parents[2]
VM_STOP = REPO_ROOT / "deploy" / "vm-stop.sh"
BUSY_CHECK = REPO_ROOT / "deploy" / "busy-check.sh"

OWNER = "someone"
DAEMON = "com.marketlake.daemon.service"
DASHBOARD = "com.marketlake.dashboard.service"
SWEEP = "com.marketlake.eod-sweep.service"
DEPLOY_SERVICE = "marketlake-deploy.service"
DAEMON_PID = "1001"

# The real tools the script and its fakes call by name, linked into a directory of their
# own so the rest of /usr/bin stays off PATH.
SYSTEM_TOOLS = ("cmp", "cp", "cut", "dirname", "grep", "mkdir", "sort")

# A window line as lake.deploy_window prints it on a refusal.
WINDOW_REFUSAL = (
    "a deploy may start next at Mon 2026-10-12 18:45 EDT, because the scheduled jobs run until then"
)

# One loginctl line each, in systemd 255's columns: SESSION, UID, USER, SEAT, TTY, STATE,
# IDLE and SINCE.
SSH_TERMINAL = "     4 1000 someone -    pts/0 active no   -\n"
TUNNEL = "     5 1000 someone -    -     active no   -\n"
CLOSING = "     6 1000 someone -    -     closing no  -\n"


@pytest.fixture(scope="module")
def tools(tmp_path_factory) -> Path:
    shared = tmp_path_factory.mktemp("stop-tools")
    install_disk_fakes(shared / "bin")
    system = shared / "system"
    system.mkdir()
    for name in SYSTEM_TOOLS:
        found = shutil.which(name, path="/usr/bin:/bin")
        assert found is not None, name
        (system / name).symlink_to(found)
    install(shared / "python", FAKE_VENV_PYTHON)
    return shared


class VM:
    """A fake VM on which every check passes, so the script would power it off."""

    def __init__(self, tmp_path: Path, tools: Path) -> None:
        self.tmp = tmp_path
        self.tools = tools
        self.root = tmp_path / "root"
        self.state = tmp_path / "state"
        self.log = tmp_path / "log"
        self.home = tmp_path / "home"
        self.checkout = self.home / "marketlake"
        for directory in (self.root, self.state, tmp_path / "tmp"):
            directory.mkdir()
        self.log.write_text("")
        deploy = self.checkout / "deploy"
        deploy.mkdir(parents=True)
        (deploy / VM_STOP.name).symlink_to(VM_STOP)
        (deploy / BUSY_CHECK.name).symlink_to(BUSY_CHECK)
        venv = self.checkout / ".venv" / "bin"
        venv.mkdir(parents=True)
        (venv / "python").symlink_to(tools / "python")
        self.conf.parent.mkdir(parents=True)
        self.conf.write_text(f"OWNER={OWNER}\nLAKE_VOLUME_ID=vol-0123456789abcdef0\n")
        self.switch.write_text("")
        self.uptime("7200.53 14000.10")
        units = self.root / cp.SYSTEMD_UNIT_DIR.lstrip("/")
        units.mkdir(parents=True)
        for unit in (DAEMON, DASHBOARD, SWEEP):
            (units / unit).write_text("")
        self.start(DAEMON, DAEMON_PID)
        self.start(DASHBOARD, "1002")
        self.procs(DAEMON_PID)
        self.env = {
            "PATH": self.path(),
            "LOG": str(self.log),
            "STATE": str(self.state),
            "FAKE_OWNER": OWNER,
            "FAKE_HOME": str(self.home),
            "TMPDIR": str(tmp_path / "tmp"),
            cp.INSTALL_ROOT_ENV: str(self.root),
            cp.INSTALL_TEST_ENV: "1",
        }

    def path(self, *first: Path) -> str:
        dirs = [*first, self.tools / "bin", self.tools / "system"]
        return ":".join(str(directory) for directory in dirs)

    @property
    def conf(self) -> Path:
        return self.root / "etc" / "marketlake" / "bootstrap.conf"

    @property
    def switch(self) -> Path:
        return self.root / "etc" / "marketlake" / "stop-when-idle"

    @property
    def script(self) -> Path:
        return self.checkout / "deploy" / VM_STOP.name

    @property
    def python(self) -> str:
        return f"{self.checkout}/.venv/bin/python"

    def uptime(self, text: str) -> None:
        proc = self.root / "proc"
        proc.mkdir(exist_ok=True)
        (proc / "uptime").write_text(f"{text}\n")

    def start(self, unit: str, pid: str) -> None:
        (self.state / "pid").mkdir(exist_ok=True)
        (self.state / "pid" / unit).write_text(pid)

    def activating(self, unit: str) -> None:
        (self.state / "activating").mkdir(exist_ok=True)
        (self.state / "activating" / unit).write_text("")

    def procs(self, *pids: str) -> None:
        group = self.root / "sys" / "fs" / "cgroup" / "system.slice" / DAEMON
        group.mkdir(parents=True, exist_ok=True)
        (group / "cgroup.procs").write_text("".join(f"{pid}\n" for pid in pids))

    def wrapped(self, name: str, body: str) -> str:
        """A PATH whose first directory holds ``name``, which runs ``body``, then the fake.

        The fake ``sudo`` keeps ``PATH``, so the wrapper answers for the owner as well.
        """
        wrap = self.tmp / f"wrap-{name}"
        wrap.mkdir()
        script = wrap / name
        script.write_text(f'#!/bin/bash\n{body}exec {self.tools / "bin" / name} "$@"\n')
        script.chmod(0o755)
        return self.path(wrap)

    def run(self, *args: str, **env: str) -> subprocess.CompletedProcess[str]:
        self.log.write_text("")
        return subprocess.run(
            ["/bin/bash", str(self.script), *args],
            cwd="/",
            env={**self.env, **env},
            capture_output=True,
            text=True,
            timeout=60,
        )

    def calls(self) -> list[str]:
        return [line for line in self.log.read_text().splitlines() if line]

    def ran(self, prefix: str) -> list[str]:
        return [line for line in self.calls() if line.startswith(prefix)]

    def powered_off(self) -> bool:
        return "systemctl poweroff" in self.calls()

    def pinged(self) -> bool:
        return any("lake.control_plane ping" in line for line in self.calls())


@pytest.fixture
def vm(tmp_path, tools) -> VM:
    return VM(tmp_path, tools)


def _refused(vm: VM, proc: subprocess.CompletedProcess[str], check: str, reason: str) -> None:
    """One line on stdout naming the check, exit 0, and no ping and no poweroff."""
    assert (proc.returncode, proc.stderr) == (0, ""), proc.stdout + proc.stderr
    assert proc.stdout == f"vm-stop: staying up, check {check}: {reason}\n"
    assert not vm.pinged(), vm.calls()
    assert not vm.powered_off(), vm.calls()


def _failed(vm: VM, proc: subprocess.CompletedProcess[str], check: str, reason: str) -> None:
    """An error is not a refusal: it exits nonzero, on stderr, and powers nothing off."""
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert proc.stderr == f"vm-stop: error, check {check}: {reason}\n"
    assert proc.stdout == ""
    assert not vm.pinged(), vm.calls()
    assert not vm.powered_off(), vm.calls()


# -- the tools -------------------------------------------------------------------------


def test_every_executable_is_a_link_to_the_dispatcher_or_a_tracked_script(tools, vm):
    shared = checked_links(tools / "bin")
    assert shared["loginctl"] == dispatcher().resolve()
    own = checked_links(vm.checkout / "deploy", VM_STOP, BUSY_CHECK)
    assert own == {VM_STOP.name: VM_STOP.resolve(), BUSY_CHECK.name: BUSY_CHECK.resolve()}


def test_the_script_is_tracked_executable_and_the_busy_check_is_not():
    staged = subprocess.run(
        ["git", "ls-files", "-s", "deploy/vm-stop.sh", "deploy/busy-check.sh"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    modes = {line.split()[-1]: line.split()[0] for line in staged.splitlines()}
    # busy-check.sh is sourced, never run.
    assert modes == {"deploy/vm-stop.sh": "100755", "deploy/busy-check.sh": "100644"}, staged
    assert os.access(VM_STOP, os.X_OK)


def test_the_unit_runs_the_script_this_test_runs():
    host = cp.SystemdHost(python="/p/python", owner="o", home="/h", project_dir="/h/m")
    assert cp.stop_unit(host).exec_start == f"/h/m/{VM_STOP.relative_to(REPO_ROOT)}"


# -- every check passes ----------------------------------------------------------------


def test_every_check_passing_takes_the_locks_last_then_pings_and_powers_off(vm):
    proc = vm.run()
    assert (proc.returncode, proc.stderr) == (0, ""), proc.stdout + proc.stderr
    assert proc.stdout.splitlines() == [
        "vm-stop: every check passed, so pinging vm-stop and powering off",
        "ping: pinged=True slug=vm-stop",
        "vm-stop: powering off",
    ]
    calls = vm.calls()
    window = f"sudo -u {OWNER} -H {vm.python} -m lake.deploy_window"
    ping = f"sudo -u {OWNER} -H {vm.python} -m lake.control_plane ping vm-stop"
    is_active = f"systemctl is-active {DEPLOY_SERVICE}"
    order = [
        "loginctl list-sessions --no-legend",
        "systemctl list-units --type=service --all --no-legend --plain com.marketlake.*",
        window,
        is_active,
        "flock -n 8",
        "flock -n 9",
        is_active,
        ping,
        "systemctl poweroff",
    ]
    picked = [line for line in calls if line in order]
    assert picked == order, calls
    # The poweroff is the last thing the script does.
    assert calls[-1] == "systemctl poweroff"
    assert (vm.root / "run" / "marketlake-deploy.lock").exists()
    assert (vm.root / "run" / "marketlake-install.lock").exists()


def test_a_failed_ping_still_powers_off(vm):
    proc = vm.run(FAKE_PING_RC="1")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "vm-stop: the vm-stop ping exited 1, and the VM powers off anyway\n" in proc.stdout
    assert vm.calls()[-1] == "systemctl poweroff"


def test_a_config_the_unit_names_reaches_the_ping_through_sudo(vm):
    proc = vm.run(MARKETLAKE_CONFIG="/srv/conf/config.yaml")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert vm.ran(f"sudo -u {OWNER} -H {vm.python} -m lake.control_plane") == [
        f"sudo -u {OWNER} -H {vm.python} -m lake.control_plane ping vm-stop"
        " --config /srv/conf/config.yaml"
    ]


def test_exactly_an_hour_up_is_enough(vm):
    vm.uptime("3600.00 100.00")
    assert vm.run().returncode == 0
    assert vm.powered_off()


def test_a_tunnel_session_with_no_terminal_does_not_hold_the_vm_up(vm):
    proc = vm.run(FAKE_SESSIONS=TUNNEL)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert vm.powered_off()


@pytest.mark.parametrize("state", ["inactive", "failed"])
def test_a_finished_deploy_does_not_hold_the_vm_up(vm, state):
    if state == "failed":
        (vm.state / "failed").mkdir()
        (vm.state / "failed" / DEPLOY_SERVICE).write_text("")
    assert vm.run().returncode == 0
    assert vm.powered_off()


# -- each check refusing on its own ----------------------------------------------------


def test_no_switch_keeps_the_vm_up_and_asks_nothing_else(vm):
    vm.switch.unlink()
    proc = vm.run()
    _refused(
        vm, proc, "1 (switch)", "/etc/marketlake/stop-when-idle does not exist, so the stop is off"
    )
    assert vm.calls() == ["id -u"]


def test_less_than_an_hour_up_keeps_the_vm_up(vm):
    vm.uptime("3599.99 100.00")
    proc = vm.run()
    _refused(vm, proc, "2 (uptime)", "the VM has been up 3599 seconds, less than 3600")
    assert not vm.ran("loginctl")


@pytest.mark.parametrize(
    ("sessions", "reason"),
    [
        (SSH_TERMINAL, "session 4 of someone is at terminal pts/0"),
        (CLOSING, "session 6 of someone is closing, so a command it started may still run"),
        (TUNNEL + SSH_TERMINAL, "session 4 of someone is at terminal pts/0"),
    ],
    ids=["terminal", "closing", "behind a tunnel"],
)
def test_a_session_at_a_terminal_or_closing_keeps_the_vm_up(vm, sessions, reason):
    proc = vm.run(FAKE_SESSIONS=sessions)
    _refused(vm, proc, "3 (terminal)", reason)
    assert not vm.ran("systemctl list-units")


def test_a_running_timer_job_keeps_the_vm_up(vm):
    vm.activating(SWEEP)
    proc = vm.run()
    _refused(vm, proc, "4 (busy)", f"{SWEEP} is activating")
    assert not vm.ran(f"sudo -u {OWNER}")


def test_a_compaction_in_the_daemons_cgroup_keeps_the_vm_up(vm):
    vm.procs(DAEMON_PID, "2002")
    proc = vm.run()
    reason = (
        f"the daemon's cgroup holds process 2002 beside its main process {DAEMON_PID},"
        " such as a compaction"
    )
    _refused(vm, proc, "4 (busy)", reason)


def test_the_scheduled_jobs_hours_keep_the_vm_up(vm):
    """The only check that keeps the stop out of capture, since the busy check skips the
    always-on daemon."""
    proc = vm.run(FAKE_WINDOW_RC="3", FAKE_WINDOW_LINE=WINDOW_REFUSAL)
    _refused(vm, proc, "5 (window)", f"inside the scheduled jobs' hours, where {WINDOW_REFUSAL}")
    assert not vm.ran("flock")
    assert not vm.ran(f"systemctl is-active {DEPLOY_SERVICE}")


@pytest.mark.parametrize("state", ["active", "activating"])
def test_a_running_deploy_keeps_the_vm_up_before_any_lock(vm, state):
    if state == "active":
        vm.start(DEPLOY_SERVICE, "3001")
    else:
        vm.activating(DEPLOY_SERVICE)
    proc = vm.run()
    _refused(vm, proc, "6 (deploy)", f"{DEPLOY_SERVICE} is {state}")
    assert not vm.ran("flock")


def test_a_held_deploy_lock_keeps_the_vm_up_and_the_install_lock_untried(vm):
    proc = vm.run(FLOCK_RCS="1")
    _refused(vm, proc, "6 (deploy)", "a deploy holds /run/marketlake-deploy.lock")
    assert vm.ran("flock") == ["flock -n 8"]


def test_a_held_install_lock_keeps_the_vm_up(vm):
    proc = vm.run(FLOCK_RCS="0 1")
    _refused(vm, proc, "6 (deploy)", "an install holds /run/marketlake-install.lock")
    assert vm.ran("flock") == ["flock -n 8", "flock -n 9"]


def test_a_deploy_that_starts_while_the_locks_are_taken_keeps_the_vm_up(vm):
    # The deploy starts as the install lock is taken, after the first check found none.
    body = (
        'if [[ "$*" == "-n 9" ]]; then\n'
        '  mkdir -p "$STATE/pid"\n'
        f'  printf 3001 > "$STATE/pid/{DEPLOY_SERVICE}"\n'
        "fi\n"
    )
    proc = vm.run(PATH=vm.wrapped("flock", body))
    _refused(vm, proc, "6 (deploy)", f"after the locks, {DEPLOY_SERVICE} is active")
    assert (
        vm.ran(f"systemctl is-active {DEPLOY_SERVICE}")
        == [f"systemctl is-active {DEPLOY_SERVICE}"] * 2
    )


# -- a check that cannot tell is an error ----------------------------------------------


@pytest.mark.parametrize(
    ("text", "files"),
    [("up", True), ("", True), ("", False)],
    ids=["not a number", "empty", "missing"],
)
def test_an_unreadable_uptime_is_an_error(vm, text, files):
    if files:
        vm.uptime(text)
    else:
        (vm.root / "proc" / "uptime").unlink()
    _failed(vm, vm.run(), "2 (uptime)", "/proc/uptime holds no uptime in seconds")


def test_a_failing_loginctl_is_an_error(vm):
    _failed(vm, vm.run(LOGINCTL_RC="1"), "3 (terminal)", "loginctl list-sessions failed")


def test_a_session_line_short_of_six_fields_is_an_error(vm):
    proc = vm.run(FAKE_SESSIONS="4 1000 someone -\n")
    _failed(vm, proc, "3 (terminal)", "loginctl printed a session with fewer than six fields")


def test_a_failing_list_units_is_an_error(vm):
    _failed(vm, vm.run(LIST_UNITS_RC="1"), "4 (busy)", "systemctl list-units failed")


def test_a_missing_busy_check_is_an_error(vm):
    (vm.checkout / "deploy" / BUSY_CHECK.name).unlink()
    proc = vm.run()
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert proc.stderr.endswith(
        "vm-stop: error, check 4 (busy): could not read deploy/busy-check.sh\n"
    )
    assert not vm.powered_off()


@pytest.mark.parametrize("code", ["1", "2", "9"])
def test_any_other_window_exit_is_an_error_not_a_refusal(vm, code):
    proc = vm.run(FAKE_WINDOW_RC=code)
    _failed(vm, proc, "5 (window)", f"python -m lake.deploy_window exited {code}")


@pytest.mark.parametrize("code", ["0", "3"])
def test_window_output_without_its_span_line_is_an_error(vm, code):
    # The fake prints one line and exits 0 for FAKE_WINDOW_BAD, so exit 3 comes from a
    # wrapper that runs the fake, keeps its output and exits 3 after it.
    path = vm.env["PATH"]
    if code == "3":
        sudo = vm.tools / "bin" / "sudo"
        path = vm.wrapped(
            "sudo", f'if [[ "$*" == *lake.deploy_window* ]]; then {sudo} "$@"; exit 3; fi\n'
        )
    proc = vm.run(FAKE_WINDOW_BAD="1", PATH=path)
    _failed(vm, proc, "5 (window)", "python -m lake.deploy_window printed no next_span_start line")


def test_a_missing_bootstrap_conf_is_an_error(vm):
    vm.conf.unlink()
    _failed(vm, vm.run(), "5 (window)", "/etc/marketlake/bootstrap.conf is missing")


@pytest.mark.parametrize(
    ("conf", "reason"),
    [
        ("OWNER=nobody-here\n", "the OWNER in bootstrap.conf names no account on this host"),
        ("OWNER=someone\nOWNER=someone\n", "bootstrap.conf sets OWNER twice"),
        ("OWNER=someone\nOTHER=1\n", "bootstrap.conf holds an unknown key"),
        ("OWNER someone\n", "bootstrap.conf holds a line that is not KEY=VALUE"),
        ("LAKE_VOLUME_ID=vol-1\n", "bootstrap.conf must set OWNER to an account name"),
    ],
    ids=["no account", "twice", "unknown key", "not a pair", "no owner"],
)
def test_a_bootstrap_conf_that_names_no_owner_is_an_error(vm, conf, reason):
    vm.conf.write_text(conf)
    _failed(vm, vm.run(), "5 (window)", reason)


def test_an_owner_the_account_database_names_differently_is_an_error(vm):
    # getent resolves a uid as well as a name, and a directory that ignores case answers
    # with the canonical name, so sudo -u would run as an account other than the one named.
    proc = vm.run(FAKE_GETENT_NAME="Someone")
    _failed(vm, proc, "5 (window)", "the OWNER in bootstrap.conf is a uid. Give the account name")


def test_a_systemctl_that_cannot_say_whether_a_deploy_runs_is_an_error(vm):
    body = 'if [[ "$1" == is-active ]]; then\n  echo "Failed to connect to bus" >&2\n  exit 1\nfi\n'
    proc = vm.run(PATH=vm.wrapped("systemctl", body))
    _failed(vm, proc, "6 (deploy)", f"systemctl is-active {DEPLOY_SERVICE} printed no state")


def test_a_failed_poweroff_is_an_error(vm):
    body = 'if [[ "$1" == poweroff ]]; then exit 1; fi\n'
    proc = vm.run(PATH=vm.wrapped("systemctl", body))
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert proc.stderr == "vm-stop: error, check poweroff: systemctl poweroff failed\n"


# -- refusing to run at all ------------------------------------------------------------


def test_an_install_root_outside_a_test_is_refused_before_any_check(vm):
    proc = vm.run(**{cp.INSTALL_TEST_ENV: ""})
    assert proc.returncode == 2
    assert proc.stderr == (
        "vm-stop: error: MARKETLAKE_INSTALL_ROOT is set, which only a test may do\n"
    )
    assert vm.calls() == []


def test_a_run_without_root_is_refused_before_any_check(vm):
    proc = vm.run(FAKE_UID="1000")
    assert proc.returncode == 2
    assert proc.stderr == "vm-stop: error: run this as root, as marketlake-stop.service does\n"
    assert vm.calls() == ["id -u"]


def test_an_argument_is_refused(vm):
    proc = vm.run("--now")
    assert proc.returncode == 2
    assert proc.stderr == "vm-stop: error: usage: vm-stop.sh, with no arguments\n"
    assert vm.calls() == []
