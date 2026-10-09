"""``deploy/vm-deploy.sh``, the hosted VM's deploy, run for real against fakes.

The script runs here under ``/bin/bash`` with ``MARKETLAKE_INSTALL_ROOT`` pointed at a
temporary directory, from ``/`` as SSM runs it. The wrapper starts the inner run through
the fake ``systemd-run`` in ``tests.support.fake_systemd``, which passes on only the
variables the wrapper names, so a variable the wrapper forgets is missing in the inner run
as it would be on the VM. Every other tool the script calls is a fake from
``tests.support.fake_systemd`` or ``tests.support.fake_disk``, except ``git``.

``git`` is real. Each test clones a bare origin built once for the module, whose ``main``
holds three commits and whose ``side`` branch holds one that is not on ``main``, and
resets the clone to the first commit. So the forward-only refusals and "Already up to
date" are checked against git itself. Only ``sudo`` stands between the script and git,
and the fake ``sudo`` clears the environment as the real one does.

``PATH`` holds the fakes, then ``needrestart``'s own directory when a test wants it, then
a directory of links to the few real tools the scripts call. ``/usr/bin`` is never on it,
so a Linux runner's own ``needrestart`` or ``systemctl`` cannot answer for a fake.

The checkout's ``deploy/vm-bootstrap.sh`` is a fake that takes its exit codes from
``BOOTSTRAP_RCS``, since ``test_vm_bootstrap.py`` runs the real one, and the rendered
``restart.sh`` is a fake that restarts through the fake ``systemctl``.
"""

from __future__ import annotations

import os
import re
import shutil
import stat
import subprocess
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lake import control_plane as cp
from lake import deploy_window
from lake.calendar import MARKET_TZ
from tests.support.clock import ManualClock
from tests.support.fake_bin import body_path, checked_links, dispatcher, install
from tests.support.fake_disk import FAKE_VENV_PYTHON, install_disk_fakes
from tests.support.fake_systemd import NEXT_RC, install_deploy_fakes

REPO_ROOT = Path(__file__).resolve().parents[2]
VM_DEPLOY = REPO_ROOT / "deploy" / "vm-deploy.sh"

OWNER = "someone"
VOLUME_ID = "vol-0123456789abcdef0"
DAEMON = "com.marketlake.daemon.service"
DASHBOARD = "com.marketlake.dashboard.service"
SWEEP = "com.marketlake.eod-sweep.service"
DEPLOY_SERVICE = "marketlake-deploy.service"
UNKNOWN = "outcome unknown: read deploy.log"
FAILED_STEP = ", with a failed step in deploy.log"
DAEMON_PID = "1001"
# A line the fake bootstrap and the fake restart.sh print, standing for a config value.
SENTINEL = "SENTINEL-FROM-THE-HOST"

# The real tools the scripts and their fakes call by name, linked into a directory of
# their own so the rest of /usr/bin stays off PATH.
SYSTEM_TOOLS = (
    "awk",
    "basename",
    "cmp",
    "cp",
    "cut",
    "date",
    "dirname",
    "find",
    "git",
    "grep",
    "head",
    "ls",
    "mkdir",
    "mkfifo",
    "od",
    "sed",
    "sort",
    "tr",
)

# The rendered restart.sh. Each argument restarts its unit through the fake systemctl,
# with an exit code from its own sequence. ``crash`` restarts the unit and leaves it
# handing out a fresh pid on every read, as a daemon that dies after three seconds does.
FAKE_RESTART_SH = (
    "#!/bin/bash\n"
    'printf \'restart.sh %s\\n\' "$*" >> "$LOG"\n'
    + NEXT_RC
    + r"""case "${1:-dashboard}" in
  daemon) rcs="${DAEMON_RESTART_RCS:-}" ;;
  dashboard) rcs="${DASHBOARD_RESTART_RCS:-}" ;;
  *) exit 2 ;;
esac
name="${1:-dashboard}"
unit="com.marketlake.$name.service"
rc="$(next_rc "$name" "$rcs")"
echo "SENTINEL-FROM-THE-HOST"
mkdir -p "$STATE/crashing"
rm -f "$STATE/crashing/$unit"
case "$rc" in
  0) systemctl restart "$unit" >/dev/null; exit $? ;;
  crash) systemctl restart "$unit" >/dev/null || exit $?; : > "$STATE/crashing/$unit"; exit 0 ;;
  *) exit "$rc" ;;
esac
"""
)

# The checkout's bootstrap. It logs the HEAD it ran at and exits with the next code in
# BOOTSTRAP_RCS. FAKE_BOOTSTRAP_BUSY names a service it leaves activating, as a timer job
# that starts during the run would be, FAKE_BOOTSTRAP_PROCS rewrites the daemon's
# cgroup.procs, and FAKE_BOOTSTRAP_TERM sends SIGTERM to the inner run the fake
# systemd-run started.
FAKE_BOOTSTRAP = (
    "#!/bin/bash\n"
    + NEXT_RC
    + r"""checkout="${0%/deploy/*}"
head="$(git -C "$checkout" rev-parse HEAD 2>/dev/null)"
printf 'bootstrap %s\n' "$head" >> "$LOG"
echo "vm-bootstrap: a fake run at $head" || true
echo "vm-bootstrap: SENTINEL-FROM-THE-HOST" >&2 || true
if [[ -n "${FAKE_BOOTSTRAP_BUSY:-}" ]]; then
  mkdir -p "$STATE/activating"
  : > "$STATE/activating/$FAKE_BOOTSTRAP_BUSY"
fi
if [[ -n "${FAKE_BOOTSTRAP_PROCS:-}" ]]; then
  group="$MARKETLAKE_INSTALL_ROOT/sys/fs/cgroup/system.slice/com.marketlake.daemon.service"
  printf '%s\n' $FAKE_BOOTSTRAP_PROCS > "$group/cgroup.procs"
fi
if [[ -n "${FAKE_BOOTSTRAP_TERM:-}" ]]; then
  kill -TERM "$(<"$STATE/inner-pid")"
fi
exit "$(next_rc bootstrap "${BOOTSTRAP_RCS:-}")"
"""
)


@dataclass(frozen=True)
class Tools:
    """The shared executables, and the origin with its commits, by name."""

    path: Path
    commits: dict[str, str]

    @property
    def origin(self) -> Path:
        return self.path / "origin.git"


def _git_env(home: Path) -> dict[str, str]:
    return {
        "PATH": "/usr/bin:/bin",
        "HOME": str(home),
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_AUTHOR_NAME": "test",
        "GIT_AUTHOR_EMAIL": "test@example.invalid",
        "GIT_COMMITTER_NAME": "test",
        "GIT_COMMITTER_EMAIL": "test@example.invalid",
    }


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        env=_git_env(cwd),
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def build_tools(shared: Path) -> Tools:
    """Every executable the tests run, and the origin each test clones."""
    bin_dir = shared / "bin"
    install_disk_fakes(bin_dir)
    # git is the real one, found in the system directory below.
    (bin_dir / "git").unlink()
    body_path(bin_dir / "git").unlink()
    install_deploy_fakes(bin_dir, shared / "needrestart")
    system = shared / "system"
    system.mkdir()
    for name in SYSTEM_TOOLS:
        found = shutil.which(name, path="/usr/bin:/bin")
        assert found is not None, name
        (system / name).symlink_to(found)
    (shared / VM_DEPLOY.name).symlink_to(VM_DEPLOY)
    install(shared / "vm-bootstrap.sh", FAKE_BOOTSTRAP)
    install(shared / "python", FAKE_VENV_PYTHON)
    install(shared / "restart.sh", FAKE_RESTART_SH)

    origin = shared / "origin.git"
    _git(shared, "init", "-q", "--bare", "-b", "main", str(origin))
    seed = shared / "seed"
    _git(shared, "init", "-q", "-b", "main", str(seed))
    (seed / ".gitignore").write_text(".venv/\n")
    deploy = seed / "deploy"
    deploy.mkdir()
    (deploy / VM_DEPLOY.name).symlink_to(shared / VM_DEPLOY.name)
    (deploy / "vm-bootstrap.sh").symlink_to(shared / "vm-bootstrap.sh")
    commits = {}
    for name in ("c1", "c2", "c3"):
        (seed / "VERSION").write_text(f"{name}\n")
        _git(seed, "add", "-A")
        _git(seed, "commit", "-q", "-m", name)
        commits[name] = _git(seed, "rev-parse", "HEAD")
    _git(seed, "remote", "add", "origin", str(origin))
    _git(seed, "push", "-q", "origin", "main")
    _git(seed, "checkout", "-q", "-b", "side", commits["c1"])
    (seed / "VERSION").write_text("side\n")
    _git(seed, "commit", "-q", "-am", "side")
    commits["side"] = _git(seed, "rev-parse", "HEAD")
    _git(seed, "push", "-q", "origin", "side")
    shutil.rmtree(seed)
    return Tools(shared, commits)


@pytest.fixture(scope="module")
def tools(tmp_path_factory) -> Tools:
    return build_tools(tmp_path_factory.mktemp("deploy-tools"))


class VM:
    """A fake VM whose checkout sits at c1, with the daemon and the dashboard running.

    No deploy is recorded, ``needrestart`` is on ``PATH`` and lists nothing, the window
    allows a deploy, and every step succeeds. A test changes what it needs first.
    """

    def __init__(self, tmp_path: Path, tools: Tools) -> None:
        self.tmp = tmp_path
        self.tools = tools
        self.c = tools.commits
        self.root = tmp_path / "root"
        self.state = tmp_path / "state"
        self.log = tmp_path / "log"
        self.home = tmp_path / "home"
        self.temp = tmp_path / "tmp"
        self.checkout = self.home / "marketlake"
        for directory in (self.root, self.state, self.temp, self.home):
            directory.mkdir()
        self.log.write_text("")
        _git(tmp_path, "clone", "-q", str(tools.origin), str(self.checkout))
        _git(self.checkout, "reset", "-q", "--hard", self.c["c1"])
        venv = self.checkout / ".venv" / "bin"
        venv.mkdir(parents=True)
        (venv / "python").symlink_to(tools.path / "python")
        self.restart_sh.parent.mkdir(parents=True)
        self.restart_sh.symlink_to(tools.path / "restart.sh")
        self.conf.parent.mkdir(parents=True)
        self.conf.write_text(f"OWNER={OWNER}\nLAKE_VOLUME_ID={VOLUME_ID}\n")
        units = self.root / cp.SYSTEMD_UNIT_DIR.lstrip("/")
        units.mkdir(parents=True)
        for unit in (DAEMON, DASHBOARD, SWEEP):
            (units / unit).write_text("")
        (self.root / "run").mkdir()
        self.start(DAEMON, DAEMON_PID)
        self.start(DASHBOARD, "1002")
        (self.state / "pidseq").write_text("1002")
        self.procs(DAEMON_PID)
        self.env = {
            "PATH": self.path(needrestart=True),
            "LOG": str(self.log),
            "STATE": str(self.state),
            "FAKE_OWNER": OWNER,
            "FAKE_HOME": str(self.home),
            "TMPDIR": str(self.temp),
            cp.INSTALL_ROOT_ENV: str(self.root),
            cp.INSTALL_TEST_ENV: "1",
        }

    def path(self, *, needrestart: bool) -> str:
        dirs = [self.tools.path / "bin"]
        if needrestart:
            dirs.append(self.tools.path / "needrestart")
        dirs.append(self.tools.path / "system")
        return ":".join(str(directory) for directory in dirs)

    # -- paths -------------------------------------------------------------------------

    @property
    def conf(self) -> Path:
        return self.root / "etc" / "marketlake" / "bootstrap.conf"

    @property
    def restart_sh(self) -> Path:
        return self.home / ".local" / "state" / "marketlake" / "systemd" / "restart.sh"

    @property
    def var(self) -> Path:
        return self.root / "var" / "lib" / "marketlake"

    @property
    def deploy_log(self) -> Path:
        return self.var / "deploy.log"

    @property
    def record_file(self) -> Path:
        return self.var / "deployed"

    @property
    def script(self) -> Path:
        return self.checkout / "deploy" / VM_DEPLOY.name

    # -- state -------------------------------------------------------------------------

    def start(self, unit: str, pid: str) -> None:
        (self.state / "pid").mkdir(exist_ok=True)
        (self.state / "pid" / unit).write_text(pid)

    def stop(self, unit: str) -> None:
        (self.state / "pid" / unit).unlink()

    def activating(self, unit: str) -> None:
        (self.state / "activating").mkdir(exist_ok=True)
        (self.state / "activating" / unit).write_text("")

    def procs(self, *pids: str) -> None:
        group = self.root / "sys" / "fs" / "cgroup" / "system.slice" / DAEMON
        group.mkdir(parents=True, exist_ok=True)
        (group / "cgroup.procs").write_text("".join(f"{pid}\n" for pid in pids))

    def record(self, sha: str) -> None:
        self.var.mkdir(parents=True, exist_ok=True)
        self.record_file.write_text(f"{sha}\n")

    def recorded(self) -> str | None:
        if not self.record_file.exists():
            return None
        return self.record_file.read_text().strip()

    def head(self) -> str:
        return _git(self.checkout, "rev-parse", "HEAD")

    # -- runs --------------------------------------------------------------------------

    def run(self, argv: list[str], **env: str) -> subprocess.CompletedProcess[str]:
        """Run the script, and check that its stdout carries nothing read from the host.

        CI prints the last line in a public log. So no run may print a line of the
        bootstrap's or restart.sh's output, the owner from bootstrap.conf, or any path
        under the test's directory, which holds the checkout, the home and every system
        path the run touched.
        """
        self.log.write_text("")
        proc = subprocess.run(
            argv,
            cwd="/",
            env={**self.env, **env},
            capture_output=True,
            text=True,
            timeout=120,
        )
        for leak in (SENTINEL, OWNER, str(self.tmp)):
            assert leak not in proc.stdout, (leak, proc.stdout)
        return proc

    def deploy(self, sha: str, *extra: str, **env: str) -> subprocess.CompletedProcess[str]:
        return self.run([str(self.script), "--sha", sha, *extra], **env)

    def calls(self) -> list[str]:
        return [line for line in self.log.read_text().splitlines() if line]

    def ran(self, prefix: str) -> list[str]:
        return [line for line in self.calls() if line.startswith(prefix)]

    def bootstraps(self) -> list[str]:
        return [line.split()[1] for line in self.ran("bootstrap ")]


@pytest.fixture
def vm(tmp_path, tools) -> VM:
    return VM(tmp_path, tools)


def _outcome(proc: subprocess.CompletedProcess[str]) -> str:
    """The wrapper's one stdout line, which must be all it printed there."""
    lines = proc.stdout.splitlines()
    assert len(lines) == 1, proc.stdout + proc.stderr
    return lines[0]


def _assert_untouched(vm: VM) -> None:
    """Nothing moved, ran or restarted."""
    calls = vm.calls()
    assert vm.head() == vm.c["c1"]
    assert not vm.ran("bootstrap "), calls
    assert not vm.ran("restart.sh"), calls
    merges = [line for line in calls if line.startswith("sudo ") and "merge --ff-only" in line]
    assert not merges, calls


# -- the tools -------------------------------------------------------------------------


def test_every_executable_is_a_link_to_the_dispatcher_or_the_script(tools, vm):
    shared = checked_links(tools.path / "bin", VM_DEPLOY)
    assert shared["systemd-run"] == dispatcher().resolve()
    assert "git" not in shared
    assert checked_links(tools.path / "needrestart") == {"needrestart": dispatcher().resolve()}
    own = checked_links(vm.checkout / "deploy", VM_DEPLOY)
    assert own[VM_DEPLOY.name] == VM_DEPLOY.resolve()
    assert own["vm-bootstrap.sh"] == dispatcher().resolve()
    assert checked_links(vm.home / ".local")["state/marketlake/systemd/restart.sh"] == (
        dispatcher().resolve()
    )


def test_the_script_is_tracked_executable():
    staged = subprocess.run(
        ["git", "ls-files", "-s", str(VM_DEPLOY.relative_to(REPO_ROOT))],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert staged.split()[0] == "100755", staged
    assert os.access(VM_DEPLOY, os.X_OK)


# -- the caps --------------------------------------------------------------------------


def _caps() -> dict[str, int]:
    text = VM_DEPLOY.read_text()
    return {
        name: int(value)
        for name, value in re.findall(r"^([A-Z_]+_SECONDS)=(\d+)\b", text, flags=re.MULTILINE)
    }


def test_the_caps_sum_to_less_than_the_window_margin():
    caps = _caps()
    budget = (
        caps["FETCH_SECONDS"]
        + 2 * caps["INSTALL_LOCK_WAIT_SECONDS"]
        + 2 * (caps["BOOTSTRAP_SECONDS"] + caps["BOOTSTRAP_KILL_SECONDS"])
        + caps["BUSY_WAIT_SECONDS"]
        + 2 * caps["RESTART_CHECK_SECONDS"]
        + caps["REST_SECONDS"]
    )
    assert budget == 200 * 60
    assert budget < deploy_window.MARGIN.total_seconds()
    # A restart's own check fits inside the share the budget gives it.
    assert caps["HOLD_SECONDS"] < caps["RESTART_CHECK_SECONDS"]
    assert caps["BUSY_WAIT_SECONDS"] % caps["POLL_SECONDS"] == 0


def test_the_script_uses_the_caps_it_names():
    text = VM_DEPLOY.read_text()
    for use in (
        'timeout "$FETCH_SECONDS"',
        'flock -w "$INSTALL_LOCK_WAIT_SECONDS"',
        'timeout -k "$BOOTSTRAP_KILL_SECONDS" "$BOOTSTRAP_SECONDS"',
        'sleep "$POLL_SECONDS"',
        'sleep "$HOLD_SECONDS"',
        "$((BUSY_WAIT_SECONDS / POLL_SECONDS))",
        "+ RESTART_CHECK_SECONDS",
    ):
        assert use in text, use


# -- deployed --------------------------------------------------------------------------


def test_a_deploy_moves_forward_restarts_and_records(vm):
    vm.record(vm.c["c1"])
    proc = vm.deploy(vm.c["c3"])
    assert proc.returncode == 0, proc.stdout + proc.stderr + vm.deploy_log.read_text()
    assert _outcome(proc) == f"deployed: {vm.c['c3']}"
    assert vm.head() == vm.c["c3"]
    assert vm.recorded() == vm.c["c3"]
    assert vm.bootstraps() == [vm.c["c3"]]
    calls = vm.calls()
    # The window runs as the owner from the current venv, before anything moves.
    window = f"sudo -u {OWNER} -H {vm.checkout}/.venv/bin/python -m lake.deploy_window"
    merge = (
        f"flock -w 600 {vm.root}/run/marketlake-install.lock sudo -u {OWNER} -H git -C"
        f" {vm.checkout} merge --ff-only {vm.c['c3']}"
    )
    fetch = f"sudo -u {OWNER} -H timeout 300 git -C {vm.checkout} fetch origin main"
    assert calls.index(window) < calls.index(fetch) < calls.index(merge)
    assert calls.index(merge) < calls.index(f"bootstrap {vm.c['c3']}")
    assert vm.ran("timeout -k 60 4500 ") == [
        f"timeout -k 60 4500 {vm.checkout}/deploy/vm-bootstrap.sh"
    ]
    assert vm.ran("restart.sh") == ["restart.sh daemon", "restart.sh dashboard"]
    assert vm.ran("sleep") == ["sleep 120"]
    assert vm.ran("flock -n") == ["flock -n 8"]
    # The record goes before the restart, and comes back through a rename.
    assert calls.index(f"rm -f -- {vm.record_file}") < calls.index("restart.sh daemon")
    assert any(line.startswith(f"mv -f -- {vm.var}/deployed.") for line in calls), calls


def test_the_wrapper_starts_the_inner_run_as_a_unit_with_its_log(vm):
    proc = vm.deploy(vm.c["c2"], "--not-after", str(int(time.time()) + 600))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    [run] = vm.ran("systemd-run ")
    assert run.startswith(
        "systemd-run --unit=marketlake-deploy --wait --collect --quiet -p OOMPolicy=continue"
        f" -p StandardOutput=append:{vm.deploy_log} -p StandardError=append:{vm.deploy_log}"
        f" --setenv=PATH={vm.env['PATH']} --setenv=MARKETLAKE_DEPLOY_RESULT={vm.root}/run/"
        "marketlake-deploy.result."
    ), run
    assert f" --setenv={cp.INSTALL_ROOT_ENV}={vm.root} --setenv={cp.INSTALL_TEST_ENV}=1 " in run
    assert run.endswith(f"{vm.script} --inner --sha {vm.c['c2']} --not-after " + run.split()[-1])
    # The inner run wrote to deploy.log and never to the journal, and its result is gone.
    assert not (vm.state / "journal").exists()
    log = vm.deploy_log.read_text()
    assert f"vm-deploy: deploying {vm.c['c2']}, invocation fake" in log
    assert "vm-bootstrap: a fake run at" in log
    assert (vm.state / "last-result").read_text() == f"0 deployed: {vm.c['c2']}\n"
    # The inner run exits 0 on every path it decides, and the code travels in the file.
    assert (vm.state / "inner-rc").read_text() == "0"
    assert list((vm.root / "run").glob("marketlake-deploy.result.*")) == []
    assert stat.S_IMODE(vm.var.stat().st_mode) == 0o700
    assert stat.S_IMODE(vm.deploy_log.stat().st_mode) == 0o600
    # A unit that has exited is no longer active, so the next deploy may start.
    assert not (vm.state / "pid" / DEPLOY_SERVICE).exists()


def test_a_wider_mode_left_by_hand_is_reset(vm):
    vm.var.mkdir(parents=True)
    vm.var.chmod(0o755)
    vm.deploy_log.write_text("an earlier run\n")
    vm.deploy_log.chmod(0o644)
    proc = vm.deploy(vm.c["c2"])
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert stat.S_IMODE(vm.var.stat().st_mode) == 0o700
    assert stat.S_IMODE(vm.deploy_log.stat().st_mode) == 0o600
    assert vm.deploy_log.read_text().startswith("an earlier run\n")


def test_only_result_files_older_than_a_day_are_deleted(vm):
    old = vm.root / "run" / "marketlake-deploy.result.old"
    fresh = vm.root / "run" / "marketlake-deploy.result.new"
    old.write_text("")
    fresh.write_text("")
    two_days_ago = time.time() - 2 * 86400
    os.utime(old, (two_days_ago, two_days_ago))
    proc = vm.deploy(vm.c["c2"])
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert not old.exists()
    assert fresh.exists()


def test_a_deploy_of_the_recorded_head_is_already_current(vm):
    vm.record(vm.c["c1"])
    proc = vm.deploy(vm.c["c1"])
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _outcome(proc) == f"deployed: {vm.c['c1']} (already current)"
    # The bootstrap runs on every deploy, the same sha included.
    assert vm.bootstraps() == [vm.c["c1"]]
    assert vm.ran("needrestart") == ["needrestart -b -r l"]
    assert not vm.ran("restart.sh")
    assert vm.recorded() == vm.c["c1"]


def test_needrestart_absent_owes_nothing_and_says_so(vm):
    vm.record(vm.c["c1"])
    proc = vm.deploy(vm.c["c1"], PATH=vm.path(needrestart=False))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _outcome(proc) == f"deployed: {vm.c['c1']} (already current)"
    assert not vm.ran("needrestart")
    assert "needrestart is not installed" in vm.deploy_log.read_text()


@pytest.mark.parametrize(
    ("env", "owed"),
    [
        ({"FAKE_NEEDRESTART_SVC": DAEMON}, True),
        ({"FAKE_NEEDRESTART_SVC": "ssh.service cron.service"}, False),
        ({"NEEDRESTART_RC": "1"}, True),
    ],
    ids=["names the daemon", "names other services", "fails"],
)
def test_needrestart_owes_a_restart_only_for_a_marketlake_service(vm, env, owed):
    vm.record(vm.c["c1"])
    proc = vm.deploy(vm.c["c1"], **env)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    if owed:
        assert _outcome(proc) == f"deployed: {vm.c['c1']}"
        assert vm.ran("restart.sh") == ["restart.sh daemon", "restart.sh dashboard"]
    else:
        assert _outcome(proc) == f"deployed: {vm.c['c1']} (already current)"
        assert not vm.ran("restart.sh")
    assert vm.recorded() == vm.c["c1"]


def test_no_record_owes_a_restart_on_the_same_sha(vm):
    proc = vm.deploy(vm.c["c1"])
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _outcome(proc) == f"deployed: {vm.c['c1']}"
    assert not vm.ran("needrestart")
    assert vm.recorded() == vm.c["c1"]


def test_a_malformed_record_reads_as_absent_and_is_never_printed(vm):
    """The record is a file on the host, so its text must not reach the public line."""
    vm.record("RECORD-TEXT-FROM-THE-HOST")
    proc = vm.deploy(vm.c["c1"], BOOTSTRAP_RCS="1")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == (
        f"not restarted: the tree is at {vm.c['c1']}, and the last recorded deploy is none"
    )
    assert "RECORD-TEXT" not in proc.stdout
    assert vm.recorded() == "RECORD-TEXT-FROM-THE-HOST"


# -- deployed, with a suffix -----------------------------------------------------------


def test_a_bootstrap_exit_4_restarts_and_writes_no_record(vm):
    vm.record(vm.c["c1"])
    proc = vm.deploy(vm.c["c3"], BOOTSTRAP_RCS="4")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == f"deployed: {vm.c['c3']}{FAILED_STEP}"
    assert vm.ran("restart.sh") == ["restart.sh daemon", "restart.sh dashboard"]
    # The daemon reads role and token_store only at start, so a later fix must still owe a
    # restart.
    assert vm.recorded() is None


def test_a_bootstrap_exit_4_with_nothing_owed_says_so(vm):
    vm.record(vm.c["c1"])
    proc = vm.deploy(vm.c["c1"], BOOTSTRAP_RCS="4")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == f"deployed: {vm.c['c1']}{FAILED_STEP}"
    assert not vm.ran("restart.sh")
    assert vm.recorded() == vm.c["c1"]


def test_a_record_that_cannot_be_written_fails_the_deploy(vm):
    proc = vm.deploy(vm.c["c2"], MV_RC="1")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == f"deployed: {vm.c['c2']}{FAILED_STEP}"
    assert vm.recorded() is None
    assert vm.ran("restart.sh") == ["restart.sh daemon", "restart.sh dashboard"]
    assert sorted(path.name for path in vm.var.iterdir()) == ["deploy.log"]


def test_a_failed_dashboard_restart_rolls_nothing_back(vm):
    proc = vm.deploy(vm.c["c3"], DASHBOARD_RESTART_RCS="1")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == f"deployed: {vm.c['c3']}, but the dashboard did not restart"
    assert vm.head() == vm.c["c3"]
    assert vm.recorded() == vm.c["c3"]
    assert vm.bootstraps() == [vm.c["c3"]]


def test_the_suffixes_come_in_their_order(vm):
    proc = vm.deploy(vm.c["c3"], BOOTSTRAP_RCS="4", DASHBOARD_RESTART_RCS="1")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == (
        f"deployed: {vm.c['c3']}{FAILED_STEP}, but the dashboard did not restart"
    )


# -- rolled back -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("rcs", "reason"),
    [
        ("1 0", "restart.sh daemon exited 1"),
        ("crash 0", "the daemon did not stay up for 120 seconds after its restart"),
    ],
    ids=["restart fails", "does not hold"],
)
def test_a_failed_daemon_restart_rolls_back_and_restarts_again(vm, rcs, reason):
    vm.record(vm.c["c1"])
    proc = vm.deploy(vm.c["c3"], DAEMON_RESTART_RCS=rcs)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == f"rolled back to {vm.c['c1']}: {reason}"
    assert vm.head() == vm.c["c1"]
    assert vm.bootstraps() == [vm.c["c3"], vm.c["c1"]]
    # The rollback restarts the daemon again and nothing else, and records what it runs.
    assert vm.ran("restart.sh") == ["restart.sh daemon", "restart.sh daemon"]
    assert vm.recorded() == vm.c["c1"]
    reset = (
        f"flock -w 600 {vm.root}/run/marketlake-install.lock sudo -u {OWNER} -H git -C"
        f" {vm.checkout} reset --keep {vm.c['c1']}"
    )
    assert reset in vm.calls()


def test_a_rollback_target_the_record_does_not_name_has_not_run_before(vm):
    vm.record(vm.c["c2"])
    proc = vm.deploy(vm.c["c3"], DAEMON_RESTART_RCS="1 0", BOOTSTRAP_RCS="0 4")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == (
        f"rolled back to {vm.c['c1']}: restart.sh daemon exited 1, which has not run"
        f" before{FAILED_STEP}"
    )
    # The rollback's bootstrap exited 4, so its restart writes no record.
    assert vm.recorded() is None
    assert vm.ran("restart.sh") == ["restart.sh daemon", "restart.sh daemon"]


def test_a_failed_bootstrap_on_a_moved_tree_rolls_back_without_a_restart(vm):
    vm.record(vm.c["c1"])
    proc = vm.deploy(vm.c["c3"], BOOTSTRAP_RCS="1 0")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == f"rolled back to {vm.c['c1']}: the bootstrap of {vm.c['c3']} exited 1"
    assert vm.head() == vm.c["c1"]
    assert vm.bootstraps() == [vm.c["c3"], vm.c["c1"]]
    assert not vm.ran("restart.sh")
    assert vm.recorded() == vm.c["c1"]
    # The busy checks run again between the failed bootstrap and the reset.
    calls = vm.calls()
    listing = "systemctl list-units --type=service --all --no-legend --plain com.marketlake.*"
    reset = next(i for i, line in enumerate(calls) if "reset --keep" in line)
    first_boot = calls.index(f"bootstrap {vm.c['c3']}")
    assert listing in calls[first_boot:reset], calls


def test_a_rollback_bootstrap_exit_4_adds_the_failed_step(vm):
    proc = vm.deploy(vm.c["c3"], BOOTSTRAP_RCS="124 4")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == (
        f"rolled back to {vm.c['c1']}: the bootstrap of {vm.c['c3']} exited 124{FAILED_STEP}"
    )


@pytest.mark.parametrize(
    ("env", "line", "record"),
    [
        (
            {"DAEMON_RESTART_RCS": "1"},
            "rollback to {c1} failed: restart.sh daemon exited 1, and then restart.sh daemon"
            " exited 1",
            None,
        ),
        (
            {"BOOTSTRAP_RCS": "1"},
            "rollback to {c1} failed: the bootstrap of {c3} exited 1, and the rollback's"
            " bootstrap exited 1",
            "c1",
        ),
        (
            {"BOOTSTRAP_RCS": "0 2", "DAEMON_RESTART_RCS": "1"},
            "rollback to {c1} failed: restart.sh daemon exited 1, and the rollback's bootstrap"
            " exited 2",
            None,
        ),
        (
            {"DAEMON_RESTART_RCS": "1 0", "FLOCK_FILE_RCS": "0 1"},
            "rollback to {c1} failed: restart.sh daemon exited 1, and the reset to it failed",
            None,
        ),
    ],
    ids=["restart again", "bootstrap again", "bootstrap after a restart", "reset"],
)
def test_a_rollback_that_fails_too_says_so(vm, env, line, record):
    """A daemon restart removes the record, and nothing after a failed rollback writes it."""
    vm.record(vm.c["c1"])
    proc = vm.deploy(vm.c["c3"], **env)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == line.format(**vm.c)
    assert vm.recorded() == (None if record is None else vm.c[record])


def test_a_failed_reset_stops_the_rollback_before_its_bootstrap(vm):
    vm.record(vm.c["c1"])
    proc = vm.deploy(vm.c["c3"], DAEMON_RESTART_RCS="1 0", FLOCK_FILE_RCS="0 1")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == (
        f"rollback to {vm.c['c1']} failed: restart.sh daemon exited 1, and the reset to it failed"
    )
    # The tree stays where the merge put it, and nothing runs or restarts on it again.
    assert vm.head() == vm.c["c3"]
    assert vm.bootstraps() == [vm.c["c3"]]
    assert vm.ran("restart.sh") == ["restart.sh daemon"]
    assert vm.recorded() is None


# -- not restarted ---------------------------------------------------------------------


def test_a_busy_service_that_outlasts_the_wait_leaves_the_restart_owed(vm):
    vm.record(vm.c["c1"])
    proc = vm.deploy(vm.c["c3"], FAKE_BOOTSTRAP_BUSY=SWEEP)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == (
        f"not restarted: the tree is at {vm.c['c3']}, and the last recorded deploy is {vm.c['c1']}"
    )
    # Fifteen minutes of polls every 30 seconds, then nothing restarts or rolls back.
    assert vm.ran("sleep") == ["sleep 30"] * 30
    assert vm.head() == vm.c["c3"]
    assert vm.bootstraps() == [vm.c["c3"]]
    assert not vm.ran("restart.sh")
    assert vm.recorded() == vm.c["c1"]
    assert f"waiting, because {SWEEP} is activating" in vm.deploy_log.read_text()


def test_a_span_too_close_for_a_restart_leaves_it_owed(vm):
    vm.record(vm.c["c1"])
    soon = str(int(time.time()) + 100)
    proc = vm.deploy(vm.c["c3"], FAKE_NEXT_SPAN=soon)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == (
        f"not restarted: the tree is at {vm.c['c3']}, and the last recorded deploy is {vm.c['c1']}"
    )
    assert not vm.ran("sleep")
    assert not vm.ran("restart.sh")
    assert vm.head() == vm.c["c3"]
    assert vm.recorded() == vm.c["c1"]


@pytest.mark.parametrize("record", [None, "c2"], ids=["no record", "another sha"])
def test_a_failed_bootstrap_on_an_unmoved_tree_leaves_an_owed_restart(vm, record):
    if record is not None:
        vm.record(vm.c[record])
    proc = vm.deploy(vm.c["c1"], BOOTSTRAP_RCS="1")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    named = "none" if record is None else vm.c[record]
    assert _outcome(proc) == (
        f"not restarted: the tree is at {vm.c['c1']}, and the last recorded deploy is {named}"
    )
    assert vm.bootstraps() == [vm.c["c1"]]
    assert not vm.ran("restart.sh")
    assert vm.recorded() == (None if record is None else vm.c[record])


# A failed bootstrap on a moved tree rolls back only once nothing reads the tree, the same
# wait the restart takes.


def test_a_busy_service_after_a_failed_bootstrap_leaves_the_tree_moved(vm):
    vm.record(vm.c["c1"])
    proc = vm.deploy(vm.c["c3"], BOOTSTRAP_RCS="1 0", FAKE_BOOTSTRAP_BUSY=SWEEP)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == (
        f"not restarted: the tree is at {vm.c['c3']}, and the last recorded deploy is {vm.c['c1']}"
    )
    assert vm.ran("sleep") == ["sleep 30"] * 30
    assert vm.head() == vm.c["c3"]
    assert vm.bootstraps() == [vm.c["c3"]]
    assert not [line for line in vm.calls() if "reset" in line]
    assert not vm.ran("restart.sh")
    assert vm.recorded() == vm.c["c1"]


def test_a_span_too_close_after_a_failed_bootstrap_leaves_the_tree_moved(vm):
    vm.record(vm.c["c1"])
    soon = str(int(time.time()) + 100)
    proc = vm.deploy(vm.c["c3"], BOOTSTRAP_RCS="1 0", FAKE_NEXT_SPAN=soon)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == (
        f"not restarted: the tree is at {vm.c['c3']}, and the last recorded deploy is {vm.c['c1']}"
    )
    assert vm.head() == vm.c["c3"]
    assert vm.bootstraps() == [vm.c["c3"]]
    assert vm.recorded() == vm.c["c1"]


def test_a_stopped_daemon_does_not_count_as_busy_in_the_wait(vm):
    """A daemon left in back-off by the bootstrap's enable --now goes to the restart."""
    vm.record(vm.c["c1"])
    # The cgroup holds a second process once the bootstrap has run, which would read as
    # busy for a running daemon.
    proc = vm.deploy(
        vm.c["c3"], FAKE_BOOTSTRAP_BUSY=DAEMON, FAKE_BOOTSTRAP_PROCS=f"{DAEMON_PID} 4242"
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _outcome(proc) == f"deployed: {vm.c['c3']}"
    assert not vm.ran("sleep 30")


# -- not deployed ----------------------------------------------------------------------


def test_a_failed_bootstrap_with_nothing_owed_is_not_deployed(vm):
    vm.record(vm.c["c1"])
    proc = vm.deploy(vm.c["c1"], BOOTSTRAP_RCS="1")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == f"not deployed: the bootstrap of {vm.c['c1']} exited 1"
    assert not vm.ran("restart.sh")


@pytest.mark.parametrize(
    ("sha", "line"),
    [
        ("side", "not deployed: {side} is not on origin/main"),
        ("never", "not deployed: {never} is not on origin/main"),
    ],
    ids=["another branch", "never fetched"],
)
def test_a_sha_not_on_main_is_refused(vm, sha, line):
    shas = {**vm.c, "never": "0123456789abcdef" * 2 + "01234567"}
    proc = vm.deploy(shas[sha])
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert _outcome(proc) == line.format(**shas)
    _assert_untouched(vm)


def test_a_sha_behind_head_is_refused(vm):
    _git(vm.checkout, "reset", "-q", "--hard", vm.c["c2"])
    proc = vm.deploy(vm.c["c1"])
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert _outcome(proc) == (
        f"not deployed: {vm.c['c1']} is behind the checkout's HEAD, {vm.c['c2']}"
    )
    assert vm.head() == vm.c["c2"]
    assert not vm.ran("bootstrap ")


def test_a_dirty_tree_is_refused(vm):
    (vm.checkout / "VERSION").write_text("edited\n")
    proc = vm.deploy(vm.c["c2"])
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert _outcome(proc) == "not deployed: the checkout has uncommitted changes"
    _assert_untouched(vm)


def test_a_branch_other_than_main_is_refused(vm):
    _git(vm.checkout, "checkout", "-q", "-b", "elsewhere")
    proc = vm.deploy(vm.c["c2"])
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert _outcome(proc) == "not deployed: the checkout is not on main"
    assert not vm.ran("bootstrap ")


def test_a_fetch_that_runs_out_of_time_is_not_deployed(vm):
    proc = vm.deploy(vm.c["c2"], FAKE_TIMEOUT_EXPIRES="git")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == ("not deployed: git fetch origin main failed or ran past 300 seconds")
    _assert_untouched(vm)


def test_an_install_lock_held_through_the_wait_is_not_deployed(vm):
    proc = vm.deploy(vm.c["c2"], FLOCK_FILE_RCS="1")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == (
        f"not deployed: the merge of {vm.c['c2']} failed, or another run held"
        " the install lock for 600 seconds"
    )
    _assert_untouched(vm)


def test_a_service_that_starts_during_the_fetch_refuses_the_merge(vm):
    proc = vm.deploy(vm.c["c2"], FAKE_TIMEOUT_BUSY=SWEEP)
    assert proc.returncode == 3, proc.stdout + proc.stderr
    assert _outcome(proc) == f"not deployed: {SWEEP} is activating"
    _assert_untouched(vm)


def _commit_that_writes_into_deploy(vm: VM) -> str:
    """Give the checkout an origin of its own whose main adds a file under deploy/.

    The new commit adds NEWFILE, changes VERSION and adds deploy/extra. git writes them in
    index order, so with deploy/ read-only the merge fails after it wrote the first two.
    """
    origin = vm.tmp / "origin-extra.git"
    _git(vm.tmp, "clone", "-q", "--bare", str(vm.tools.origin), str(origin))
    work = vm.tmp / "work-extra"
    _git(vm.tmp, "clone", "-q", str(origin), str(work))
    (work / "NEWFILE").write_text("extra\n")
    (work / "VERSION").write_text("extra\n")
    (work / "deploy" / "extra").write_text("extra\n")
    _git(work, "add", "-A")
    _git(work, "commit", "-q", "-m", "extra")
    _git(work, "push", "-q", "origin", "main")
    _git(vm.checkout, "remote", "set-url", "origin", str(origin))
    return _git(work, "rev-parse", "HEAD")


@pytest.mark.parametrize("restored", [True, False], ids=["restored", "restore fails"])
@pytest.mark.skipif(os.geteuid() == 0, reason="root writes into a read-only directory")
def test_a_merge_that_fails_partway_restores_the_tree(vm, restored):
    sha = _commit_that_writes_into_deploy(vm)
    deploy = vm.checkout / "deploy"
    deploy.chmod(0o555)
    try:
        env = {} if restored else {"FLOCK_FILE_RCS": "0 1"}
        proc = vm.deploy(sha, **env)
    finally:
        deploy.chmod(0o755)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert vm.head() == vm.c["c1"]
    assert not vm.ran("bootstrap ")
    assert not vm.ran("restart.sh")
    status = _git(vm.checkout, "status", "--porcelain")
    if restored:
        assert _outcome(proc) == (
            f"not deployed: the merge of {sha} failed, and the tree was restored"
        )
        assert status == ""
        assert (vm.checkout / "VERSION").read_text() == "c1\n"
        assert not (vm.checkout / "NEWFILE").exists()
        # The ignored venv survives the clean.
        assert (vm.checkout / ".venv" / "bin" / "python").is_symlink()
    else:
        assert _outcome(proc) == UNKNOWN
        # The merge really did write files before it failed.
        assert "?? NEWFILE" in status.splitlines()
        assert "M VERSION" in status


def test_a_head_moved_elsewhere_by_the_merge_is_an_unknown_outcome(vm):
    """A post-merge hook stands for anything else that moves HEAD under the merge."""
    hook = vm.checkout / ".git" / "hooks" / "post-merge"
    hook.write_text(f"#!/bin/sh\nexec git reset -q --hard {vm.c['c2']}\n")
    hook.chmod(0o755)
    proc = vm.deploy(vm.c["c3"])
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == UNKNOWN
    assert vm.head() == vm.c["c2"]
    assert not vm.ran("bootstrap ")
    assert not vm.ran("restart.sh")


@pytest.mark.parametrize("state", ["stopped", "activating", "failed"])
def test_a_daemon_that_is_not_active_is_left_alone(vm, state):
    vm.stop(DAEMON)
    if state == "activating":
        vm.activating(DAEMON)
    elif state == "failed":
        (vm.state / "failed").mkdir(exist_ok=True)
        (vm.state / "failed" / DAEMON).write_text("")
    proc = vm.deploy(vm.c["c2"])
    assert proc.returncode == 2, proc.stdout + proc.stderr
    expected = {"stopped": "inactive"}.get(state, state)
    assert _outcome(proc).startswith(f"not deployed: {DAEMON} is {expected}, not active.")
    # It is checked before the window, so no Python starts.
    assert not vm.ran("venv-python")
    _assert_untouched(vm)


def test_the_window_refusal_is_the_last_line(vm):
    line = "a deploy may start next at Mon 2026-10-12 18:45 EDT, because the jobs run"
    proc = vm.deploy(vm.c["c2"], FAKE_WINDOW_RC="3", FAKE_WINDOW_LINE=line)
    assert proc.returncode == 3, proc.stdout + proc.stderr
    assert _outcome(proc) == f"not deployed: {line}"
    assert not vm.ran("sudo -u someone -H timeout")
    _assert_untouched(vm)


@pytest.mark.parametrize(
    ("env", "line"),
    [
        ({"FAKE_WINDOW_RC": "1"}, "not deployed: python -m lake.deploy_window exited 1"),
        (
            {"FAKE_WINDOW_BAD": "1"},
            "not deployed: python -m lake.deploy_window printed no next_span_start line",
        ),
        (
            {"FAKE_NEXT_SPAN": "soon"},
            "not deployed: python -m lake.deploy_window printed no next_span_start line",
        ),
        (
            {"FAKE_NEXT_SPAN": "04102444800"},
            "not deployed: python -m lake.deploy_window printed no next_span_start line",
        ),
    ],
    ids=["exit 1", "one line", "not a number", "a leading zero"],
)
def test_a_window_check_that_fails_is_not_deployed(vm, env, line):
    proc = vm.deploy(vm.c["c2"], **env)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == line
    _assert_untouched(vm)


def test_a_running_timer_service_refuses_the_deploy(vm):
    vm.activating(SWEEP)
    proc = vm.deploy(vm.c["c2"])
    assert proc.returncode == 3, proc.stdout + proc.stderr
    assert _outcome(proc) == f"not deployed: {SWEEP} is activating"
    _assert_untouched(vm)


def test_a_waiting_timer_does_not_refuse_the_deploy(vm):
    """A timer reads active while it waits, so only services are listed."""
    vm.start("com.marketlake.eod-sweep.timer", "1500")
    proc = vm.deploy(vm.c["c2"])
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "systemctl list-units --type=service --all --no-legend --plain com.marketlake.*" in (
        vm.calls()
    )


def test_a_second_process_in_the_daemons_cgroup_refuses_the_deploy(vm):
    vm.procs(DAEMON_PID, "4242")
    proc = vm.deploy(vm.c["c2"])
    assert proc.returncode == 3, proc.stdout + proc.stderr
    assert _outcome(proc) == (
        "not deployed: the daemon's cgroup holds process 4242 beside its main process"
        f" {DAEMON_PID}, such as a compaction"
    )
    _assert_untouched(vm)


def test_the_daemons_threads_alone_do_not_refuse_the_deploy(vm):
    vm.procs(DAEMON_PID)
    proc = vm.deploy(vm.c["c2"])
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert f"systemctl show -p ActiveState,MainPID,ControlGroup {DAEMON}" in vm.calls()


def test_an_unreadable_cgroup_is_not_deployed(vm):
    (vm.root / "sys" / "fs" / "cgroup" / "system.slice" / DAEMON / "cgroup.procs").unlink()
    proc = vm.deploy(vm.c["c2"])
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc).startswith("not deployed: could not read ")
    _assert_untouched(vm)


def test_another_deploy_running_refuses_before_the_unit_starts(vm):
    vm.start(DEPLOY_SERVICE, "77")
    proc = vm.deploy(vm.c["c2"])
    assert proc.returncode == 3, proc.stdout + proc.stderr
    assert _outcome(proc) == "not deployed: another deploy is running"
    assert not vm.ran("systemd-run")


def test_a_unit_another_wrapper_started_first_refuses(vm):
    proc = vm.deploy(vm.c["c2"], FAKE_SYSTEMD_RUN="taken")
    assert proc.returncode == 3, proc.stdout + proc.stderr
    assert _outcome(proc) == "not deployed: another deploy is running"
    _assert_untouched(vm)


def test_a_held_deploy_lock_refuses_the_inner_run(vm):
    proc = vm.deploy(vm.c["c2"], FLOCK_RC="1")
    assert proc.returncode == 3, proc.stdout + proc.stderr
    assert _outcome(proc) == "not deployed: another deploy is running"
    _assert_untouched(vm)


def test_an_expired_request_is_refused_before_anything_runs(vm):
    proc = vm.deploy(vm.c["c2"], "--not-after", str(int(time.time()) - 1))
    assert proc.returncode == 3, proc.stdout + proc.stderr
    assert _outcome(proc) == "not deployed: the request expired"
    assert not vm.ran("systemd-run")


def test_the_inner_run_checks_the_expiry_again(vm):
    argv = [str(vm.script), "--inner", "--sha", vm.c["c2"], "--not-after", "1000000000"]
    proc = vm.run(argv, INVOCATION_ID="direct")
    assert proc.returncode == 3, proc.stdout + proc.stderr
    assert _outcome(proc) == "not deployed: the request expired"


def test_an_inner_run_outside_a_unit_is_refused(vm):
    proc = vm.run([str(vm.script), "--inner", "--sha", vm.c["c2"]])
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert _outcome(proc) == "not deployed: --inner runs only as the unit the wrapper starts"
    _assert_untouched(vm)


@pytest.mark.parametrize(
    ("argv", "line"),
    [
        ([], "not deployed: --sha must be 40 lowercase hex digits"),
        (["--sha"], None),
        (["--sha", "ABC"], "not deployed: --sha must be 40 lowercase hex digits"),
        (
            ["--sha", "a" * 40, "--not-after", "soon"],
            "not deployed: --not-after must be epoch seconds",
        ),
        # bash reads a leading zero as octal, and 0999 is no octal number.
        (
            ["--sha", "a" * 40, "--not-after", "0999"],
            "not deployed: --not-after must be epoch seconds",
        ),
        (["--sha", "a" * 40, "--help"], None),
    ],
    ids=["nothing", "no value", "not hex", "bad expiry", "octal expiry", "unknown flag"],
)
def test_a_usage_error_is_one_line_on_stdout(vm, argv, line):
    proc = vm.run([str(vm.script), *argv])
    assert proc.returncode == 2, proc.stdout + proc.stderr
    usage = "not deployed: usage: vm-deploy.sh --sha <40 hex digits> [--not-after <epoch seconds>]"
    assert _outcome(proc) == (line or usage)
    assert vm.calls() == []


@pytest.mark.parametrize(
    ("env", "line"),
    [
        ({"FAKE_UID": "1000"}, "not deployed: run this as root, for example with sudo"),
        (
            {cp.INSTALL_TEST_ENV: ""},
            "not deployed: MARKETLAKE_INSTALL_ROOT is set, which only a test may do",
        ),
    ],
    ids=["not root", "leaked root"],
)
def test_a_bad_host_is_refused(vm, env, line):
    proc = vm.deploy(vm.c["c2"], **env)
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert _outcome(proc) == line.format(root=vm.root)
    assert not vm.ran("systemd-run")


# -- outcome unknown -------------------------------------------------------------------


def test_a_unit_that_never_ran_is_an_unknown_outcome(vm):
    proc = vm.deploy(vm.c["c2"], FAKE_SYSTEMD_RUN="fail")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == UNKNOWN
    assert list((vm.root / "run").glob("marketlake-deploy.result.*")) == []


@pytest.mark.parametrize(
    "content",
    [
        "7 deployed: x\\n",
        "194 deployed: x\\n",
        "deployed\\n",
        "1\\n",
        "1 not deployed: x\\nmore\\n",
        "1 \\n",
    ],
    ids=["code 7", "code 194", "no code", "code only", "two lines", "no line"],
)
def test_a_malformed_result_is_an_unknown_outcome(vm, content):
    proc = vm.deploy(vm.c["c2"], FAKE_SYSTEMD_RUN="result", FAKE_RESULT=content)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == UNKNOWN


@pytest.mark.parametrize("code", ["1", "2", "3"])
def test_a_failure_code_carries_its_line(vm, code):
    proc = vm.deploy(vm.c["c2"], FAKE_SYSTEMD_RUN="result", FAKE_RESULT=f"{code} a line\\n")
    assert proc.returncode == int(code), proc.stdout + proc.stderr
    assert _outcome(proc) == "a line"


@pytest.mark.parametrize("suffix", ["", " (already current)"], ids=["bare", "current"])
def test_exit_0_carries_a_deployed_line_for_the_requested_sha(vm, suffix):
    line = f"deployed: {vm.c['c2']}{suffix}"
    proc = vm.deploy(vm.c["c2"], FAKE_SYSTEMD_RUN="result", FAKE_RESULT=f"0 {line}\\n")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _outcome(proc) == line


@pytest.mark.parametrize(
    ("code", "line"),
    [
        ("0", "not deployed: a reason"),
        ("0", "deployed: {c3}"),
        ("0", "deployed: {c2}, with a failed step in deploy.log"),
        ("1", "deployed: {c2}"),
        ("3", "deployed: {c2} (already current)"),
    ],
    ids=["0 with a refusal", "0 for another sha", "0 with a suffix", "1 bare", "3 current"],
)
def test_any_other_pairing_of_code_and_line_is_unknown(vm, code, line):
    content = f"{code} {line.format(**vm.c)}\\n"
    proc = vm.deploy(vm.c["c2"], FAKE_SYSTEMD_RUN="result", FAKE_RESULT=content)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == UNKNOWN


@pytest.mark.parametrize(
    "eastern",
    [
        datetime(2026, 10, 12, 12, 0),  # a Monday inside the weekday span, in EDT
        datetime(2026, 10, 12, 6, 0),  # less than the margin before that span
        datetime(2026, 12, 13, 21, 0),  # inside the Sunday span, in EST
    ],
    ids=["in a span", "in the margin", "in winter"],
)
def test_the_real_window_refusal_has_the_shape_the_script_repeats(capsys, eastern):
    """The script passes the window's line to the public log only when it has this shape."""
    pattern = re.search(r"^WINDOW_LINE_RE='(.+)'$", VM_DEPLOY.read_text(), flags=re.MULTILINE)
    assert pattern is not None
    now = eastern.replace(tzinfo=MARKET_TZ).astimezone(UTC)
    assert deploy_window.main([], clock=ManualClock(now)) == deploy_window.REFUSED
    first = capsys.readouterr().out.splitlines()[0]
    assert re.fullmatch(pattern.group(1), first), first


def test_a_window_line_of_another_shape_is_not_repeated(vm):
    proc = vm.deploy(vm.c["c2"], FAKE_WINDOW_RC="3", FAKE_WINDOW_LINE=f"refused, see {vm.root}/etc")
    assert proc.returncode == 3, proc.stdout + proc.stderr
    assert _outcome(proc) == "not deployed: the window refuses a deploy now, see deploy.log"


def test_an_owner_with_no_account_is_not_named(vm):
    vm.conf.write_text(f"OWNER=nobody-here\nLAKE_VOLUME_ID={VOLUME_ID}\n")
    proc = vm.deploy(vm.c["c2"])
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert _outcome(proc) == (
        "not deployed: the OWNER in bootstrap.conf names no account on this host"
    )


@pytest.mark.parametrize(
    ("conf", "line"),
    [
        (None, "not deployed: /etc/marketlake/bootstrap.conf is missing"),
        (
            f"OWNER={OWNER}\nnot a pair\n",
            "not deployed: bootstrap.conf holds a line that is not KEY=VALUE",
        ),
        (
            f"OWNER={OWNER}\nOWNER={OWNER}\n",
            "not deployed: bootstrap.conf sets OWNER twice",
        ),
        (
            f"OWNER={OWNER}\nLAKE_VOLUME_ID={VOLUME_ID}\nLAKE_VOLUME_ID={VOLUME_ID}\n",
            "not deployed: bootstrap.conf sets LAKE_VOLUME_ID twice",
        ),
        (
            f"OWNER={OWNER}\nREGION=us-east-1\n",
            "not deployed: bootstrap.conf holds an unknown key",
        ),
        (
            f"LAKE_VOLUME_ID={VOLUME_ID}\n",
            "not deployed: bootstrap.conf must set OWNER to an account name",
        ),
        (
            "OWNER=1000\n",
            "not deployed: bootstrap.conf must set OWNER to an account name",
        ),
    ],
    ids=["missing", "not a pair", "owner twice", "volume twice", "unknown key", "no owner", "uid"],
)
def test_a_bootstrap_conf_it_cannot_use_is_refused(vm, conf, line):
    if conf is None:
        vm.conf.unlink()
    else:
        vm.conf.write_text(conf)
    proc = vm.deploy(vm.c["c2"])
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert _outcome(proc) == line
    assert not vm.ran("systemd-run")


@pytest.mark.parametrize(
    ("env", "line"),
    [
        (
            {"FAKE_GETENT_NAME": "Someone"},
            "not deployed: the OWNER in bootstrap.conf is a uid. Give the account name",
        ),
        (
            {"FAKE_GETENT_HOME": ""},
            "not deployed: the OWNER in bootstrap.conf has no home directory",
        ),
    ],
    ids=["another name", "no home"],
)
def test_an_account_entry_it_cannot_use_is_refused(vm, env, line):
    proc = vm.deploy(vm.c["c2"], **env)
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert _outcome(proc) == line
    assert not vm.ran("systemd-run")


def test_a_state_directory_it_cannot_create_is_not_deployed(vm):
    (vm.root / "var" / "lib").mkdir(parents=True)
    vm.var.write_text("a file where the directory goes\n")
    proc = vm.deploy(vm.c["c2"])
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == "not deployed: could not create /var/lib/marketlake at mode 0700"
    assert not vm.ran("systemd-run")


def test_a_deploy_log_it_cannot_create_is_not_deployed(vm):
    vm.deploy_log.mkdir(parents=True)
    proc = vm.deploy(vm.c["c2"])
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == "not deployed: could not create deploy.log at mode 0600"
    assert not vm.ran("systemd-run")


@pytest.mark.skipif(os.geteuid() == 0, reason="root writes into a read-only directory")
def test_a_result_file_it_cannot_create_is_not_deployed(vm):
    run = vm.root / "run"
    run.chmod(0o555)
    try:
        proc = vm.deploy(vm.c["c2"])
    finally:
        run.chmod(0o755)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == "not deployed: could not create a result file in /run"
    assert not vm.ran("systemd-run")


def test_a_daemon_show_that_fails_is_not_deployed(vm):
    proc = vm.deploy(vm.c["c2"], FAIL_SHOW=DAEMON)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == f"not deployed: systemctl show failed for {DAEMON}"
    _assert_untouched(vm)


def test_a_main_pid_of_0_after_a_restart_is_not_running(vm):
    """systemd reads MainPID=0 for an active unit whose process has not started yet."""
    vm.record(vm.c["c1"])
    proc = vm.deploy(vm.c["c3"], RESTART_DELAY="1")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    reason = "the daemon was not running after its restart"
    assert _outcome(proc) == f"rollback to {vm.c['c1']} failed: {reason}, and then {reason}"
    # The first read after the restart answers 0, so no hold check sleeps.
    assert not vm.ran("sleep 120")
    assert vm.recorded() is None


def test_a_signal_during_the_run_reports_an_unknown_outcome(vm):
    proc = vm.deploy(vm.c["c3"], FAKE_BOOTSTRAP_TERM="1")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _outcome(proc) == UNKNOWN
    assert (vm.state / "last-result").read_text() == f"1 {UNKNOWN}\n"
    # The trap exits 1. Killed by the signal, bash would exit 143.
    assert (vm.state / "inner-rc").read_text() == "1"
    # The run stopped after the merge and before any restart.
    assert vm.head() == vm.c["c3"]
    assert not vm.ran("restart.sh")


# -- a log that cannot be written ------------------------------------------------------


def test_a_broken_log_stops_nothing(vm):
    vm.record(vm.c["c1"])
    proc = vm.deploy(vm.c["c3"], FAKE_BROKEN_PIPE="1")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _outcome(proc) == f"deployed: {vm.c['c3']}"
    assert vm.bootstraps() == [vm.c["c3"]]
    assert vm.ran("restart.sh") == ["restart.sh daemon", "restart.sh dashboard"]
    assert vm.recorded() == vm.c["c3"]
    # Every line went to the broken pipe, so the log holds nothing from this run.
    assert vm.deploy_log.read_text() == ""
