"""The deploy document's one shell step, run under dash against fakes (#676).

``infra/live/deploy.tf`` reads ``infra/live/deploy-step.sh`` into the SSM document
``marketlake-deploy`` with ``file()``. The SSM agent substitutes ``{{ sha }}`` and
``{{ notAfter }}`` and runs the step as root with ``sh``, which is dash on Ubuntu. The test
substitutes both the same way and runs the result with ``/bin/dash`` by name, because a
Mac's ``/bin/sh`` is bash and accepts what dash refuses.

The step sets its own ``PATH`` and reads ``/etc/marketlake/bootstrap.conf`` by its absolute
path, so the test rewrites those two literals to reach the fakes and a temporary root.
``test_the_rewrite_leaves_no_system_path`` checks that no other absolute path survives.
``getent`` is ``tests.support.fake_systemd``'s fake, and the checkout's
``deploy/vm-deploy.sh`` is a fake that logs how it was run and prints a last line.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

from tests.support.fake_bin import checked_links, dispatcher, install
from tests.support.fake_systemd import FAKE_GETENT

REPO_ROOT = Path(__file__).resolve().parents[2]
STEP = REPO_ROOT / "infra" / "live" / "deploy-step.sh"
DASH = Path("/bin/dash")

OWNER = "someone"
SHA = "0123456789abcdef0123456789abcdef01234567"
NOT_AFTER = "1791234567"
CONF_DIR = "/etc/marketlake"
STEP_PATH = "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

# The checkout's vm-deploy.sh. It keeps its argv, pid, directory and PATH, prints the
# last line VM_DEPLOY_LINE names and exits VM_DEPLOY_RC, as the host's wrapper does.
FAKE_VM_DEPLOY = r"""#!/bin/bash
printf '%s\n' "$@" > "$STATE/argv"
printf '%s' "$$" > "$STATE/pid"
pwd > "$STATE/cwd"
printf '%s' "$PATH" > "$STATE/path"
echo "${VM_DEPLOY_LINE:-deployed: $4}"
exit "${VM_DEPLOY_RC:-0}"
"""

pytestmark = pytest.mark.skipif(not DASH.exists(), reason="the step runs under /bin/dash")


def substituted(sha: str = SHA, not_after: str = NOT_AFTER) -> str:
    """The step as the agent runs it, with both parameters in place."""
    text = STEP.read_text()
    assert text.count("{{ sha }}") == 1 and text.count("{{ notAfter }}") == 1
    text = text.replace("{{ sha }}", sha).replace("{{ notAfter }}", not_after)
    assert "{{" not in text and "}}" not in text
    return text


@pytest.fixture(scope="module")
def tools(tmp_path_factory) -> Path:
    shared = tmp_path_factory.mktemp("deploy-step-tools")
    install(shared / "bin" / "getent", FAKE_GETENT)
    install(shared / "vm-deploy.sh", FAKE_VM_DEPLOY)
    return shared


class Host:
    """A VM with bootstrap.conf naming the owner, and the owner's checkout in place."""

    def __init__(self, tmp_path: Path, tools: Path) -> None:
        self.tools = tools
        self.root = tmp_path / "root"
        self.home = tmp_path / "home"
        self.state = tmp_path / "state"
        for directory in (self.root, self.home, self.state):
            directory.mkdir()
        self.conf_dir = self.root / CONF_DIR.lstrip("/")
        self.conf_dir.mkdir(parents=True)
        self.conf.write_text(f"OWNER={OWNER}\nLAKE_VOLUME_ID=vol-0fedcba9876543210\n")
        self.script_path = f"{tools / 'bin'}:/usr/bin:/bin"
        self.script = tmp_path / "step.sh"
        self.write_step(substituted())
        deploy = self.home / "marketlake" / "deploy"
        deploy.mkdir(parents=True)
        (deploy / "vm-deploy.sh").symlink_to(tools / "vm-deploy.sh")
        self.env = {
            "PATH": "/nowhere",
            "STATE": str(self.state),
            "LOG": str(tmp_path / "log"),
            "FAKE_OWNER": OWNER,
            "FAKE_HOME": str(self.home),
        }

    @property
    def conf(self) -> Path:
        return self.conf_dir / "bootstrap.conf"

    def write_step(self, text: str) -> None:
        assert text.count(STEP_PATH + "\n") == 1
        text = text.replace(STEP_PATH + "\n", f"PATH={self.script_path}\n")
        self.script.write_text(text.replace(CONF_DIR, str(self.conf_dir)))

    def run(self, **env: str) -> tuple[subprocess.CompletedProcess[str], int]:
        proc = subprocess.Popen(
            [str(DASH), str(self.script)],
            env={**self.env, **env},
            cwd=self.state,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout, stderr = proc.communicate(timeout=60)
        return subprocess.CompletedProcess(proc.args, proc.returncode, stdout, stderr), proc.pid

    def ran(self) -> bool:
        return (self.state / "argv").exists()


@pytest.fixture
def host(tmp_path, tools) -> Host:
    return Host(tmp_path, tools)


def _refused(proc: subprocess.CompletedProcess[str], host: Host, reason: str) -> None:
    """One ``not deployed:`` line on stdout, exit 2, and no deploy."""
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert proc.stdout.splitlines() == [f"not deployed: {reason}"], proc.stdout
    assert proc.stderr == ""
    assert not host.ran()


# -- the harness -----------------------------------------------------------------------


def test_every_executable_is_a_link_to_the_dispatcher(tmp_path, tools, host):
    assert checked_links(tools)["vm-deploy.sh"] == dispatcher().resolve()
    own = checked_links(tmp_path)
    assert own["home/marketlake/deploy/vm-deploy.sh"] == dispatcher().resolve()


def test_the_rewrite_leaves_no_system_path(host):
    """Every absolute path the step names is one the test moves, one it only reads, or
    the root the step changes to, so a run cannot write outside the test's root."""
    text = substituted()
    # The PATH line's later entries follow a colon, and the checkout's path follows
    # $home, so neither starts a match here. The PATH line is rewritten whole.
    paths = set(re.findall(r"(?<![\w.:/$])/[\w./-]*", text))
    assert paths == {"/bin/sh", "/usr/local/sbin", "/", CONF_DIR + "/bootstrap.conf"}, paths
    written = host.script.read_text()
    assert CONF_DIR not in written.replace(str(host.conf_dir), "")
    assert "/usr/local/sbin" not in written


def test_dash_refuses_a_bashism():
    """The step runs under a shell that refuses what bash accepts, as the agent's does."""
    proc = subprocess.run(
        [str(DASH), "-c", "[[ 1 == 1 ]]"], capture_output=True, text=True, timeout=60
    )
    assert proc.returncode != 0


# -- the hand-over ---------------------------------------------------------------------


@pytest.mark.parametrize("rc", [0, 1, 2, 3])
def test_the_step_runs_vm_deploy_with_the_two_parameters(host, rc):
    line = "deployed: " + SHA if rc == 0 else "not deployed: outside the deploy hours"
    proc, pid = host.run(VM_DEPLOY_RC=str(rc), VM_DEPLOY_LINE=line)
    assert proc.returncode == rc, proc.stdout + proc.stderr
    assert proc.stdout == line + "\n"
    assert proc.stderr == ""
    assert (host.state / "argv").read_text().splitlines() == [
        "--not-after",
        NOT_AFTER,
        "--sha",
        SHA,
    ]
    # exec replaces the step's shell, so the exit code is vm-deploy.sh's own.
    assert (host.state / "pid").read_text() == str(pid)
    assert (host.state / "cwd").read_text() == "/\n"
    assert (host.state / "path").read_text() == host.script_path


def test_a_newline_after_the_sha_cuts_off_nothing_the_host_needs(host):
    """``--sha`` comes last. Were a stray newline ever to follow the substituted sha,
    the line would still end on it, and ``--not-after`` would not drop into a command of
    its own while the deploy ran without an expiry."""
    host.write_step(substituted(sha=SHA + "\n"))
    proc, _ = host.run()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert (host.state / "argv").read_text().splitlines() == [
        "--not-after",
        NOT_AFTER,
        "--sha",
        SHA,
    ]


@pytest.mark.parametrize("owner", ["some.one-x", "_svc", "a_b.c-d9"])
def test_an_owner_with_dots_dashes_and_underscores_is_accepted(host, owner):
    """The account rule vm-bootstrap.sh applies allows these characters."""
    host.conf.write_text(f"OWNER={owner}\nLAKE_VOLUME_ID=vol-0fedcba9876543210\n")
    proc, _ = host.run(FAKE_OWNER=owner)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert host.ran()


def test_a_conf_without_a_final_newline_is_read(host):
    host.conf.write_text(f"LAKE_VOLUME_ID=vol-0fedcba9876543210\nOWNER={OWNER}")
    proc, _ = host.run()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert host.ran()


def test_blank_lines_in_the_conf_are_skipped(host):
    host.conf.write_text(f"\nOWNER={OWNER}\n\nLAKE_VOLUME_ID=vol-0fedcba9876543210\n")
    proc, _ = host.run()
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_a_missing_vm_deploy_exits_127(host):
    """send-deploy.sh reads 127 as a VM that needs the manual first deploy."""
    (host.home / "marketlake" / "deploy" / "vm-deploy.sh").unlink()
    proc, _ = host.run()
    assert proc.returncode == 127, proc.stdout + proc.stderr
    assert proc.stdout == ""


# -- the refusals ----------------------------------------------------------------------


def test_a_missing_conf_is_refused(host):
    host.conf.unlink()
    proc, _ = host.run()
    _refused(proc, host, f"{host.conf} is missing, so the deploy cannot find the checkout")


def test_a_conf_that_is_a_directory_is_refused(host):
    host.conf.unlink()
    host.conf.mkdir()
    proc, _ = host.run()
    _refused(proc, host, f"{host.conf} is missing, so the deploy cannot find the checkout")


@pytest.mark.parametrize(
    ("text", "reason"),
    [
        (f"OWNER={OWNER}\nOWNER=other\n", "{conf} sets OWNER twice"),
        (f"OWNER={OWNER}\nEXTRA=1\n", "{conf} holds a line other than OWNER and LAKE_VOLUME_ID"),
        (
            f"OWNER={OWNER}\n$(touch pwned)\n",
            "{conf} holds a line other than OWNER and LAKE_VOLUME_ID",
        ),
        ("LAKE_VOLUME_ID=vol-0fedcba9876543210\n", "{conf} does not set OWNER to an account name"),
        ("OWNER=\n", "{conf} does not set OWNER to an account name"),
        ("OWNER=1000\n", "{conf} does not set OWNER to an account name"),
        ("OWNER=-u\n", "{conf} does not set OWNER to an account name"),
        ("OWNER=some one\n", "{conf} does not set OWNER to an account name"),
        ("OWNER=some/one\n", "{conf} does not set OWNER to an account name"),
        ("OWNER=$(id)\n", "{conf} does not set OWNER to an account name"),
    ],
    ids=[
        "owner-twice",
        "unknown-key",
        "command-line",
        "no-owner",
        "empty-owner",
        "uid",
        "option",
        "space",
        "slash",
        "substitution",
    ],
)
def test_a_malformed_conf_is_refused(host, text, reason):
    host.conf.write_text(text)
    proc, _ = host.run()
    _refused(proc, host, reason.format(conf=host.conf))
    assert not (host.state / "pwned").exists()


def test_an_owner_with_no_account_is_refused(host):
    proc, _ = host.run(FAKE_OWNER="nobody-else")
    _refused(proc, host, "the OWNER in bootstrap.conf names no account on this host")


def test_an_account_with_no_home_is_refused(host):
    proc, _ = host.run(FAKE_GETENT_HOME="")
    _refused(proc, host, "the OWNER in bootstrap.conf has no home directory")


@pytest.mark.parametrize(
    ("text", "env"),
    [
        ("OWNER={name}\n", {"FAKE_OWNER": "nobody-else"}),
        ("OWNER={name}\n", {"FAKE_OWNER": "{name}", "FAKE_GETENT_HOME": ""}),
        ("OWNER={name}\nOWNER={name}\n", {"FAKE_OWNER": "{name}"}),
        ("OWNER={name}\nEXTRA={name}\n", {"FAKE_OWNER": "{name}"}),
        ("OWNER={name}/x\n", {"FAKE_OWNER": "{name}"}),
    ],
    ids=["no-account", "no-home", "owner-twice", "unknown-key", "bad-name"],
)
def test_a_refusal_never_prints_a_config_value(host, text, env):
    """send-deploy.sh prints the step's line in a public log, so the line holds only the
    step's own words, never the owner's name or any other value from the conf."""
    name = "zq7owner-sentinel"
    host.conf.write_text(text.format(name=name))
    proc, _ = host.run(**{key: value.format(name=name) for key, value in env.items()})
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert proc.stdout.startswith("not deployed: ")
    assert name not in proc.stdout + proc.stderr
