"""The hosted VM's first-boot shim, rendered and run against fakes.

``infra/live/vm.tf`` renders ``infra/live/user-data.sh.tftpl`` with ``templatefile`` into
the instance's ``user_data``, and cloud-init runs it once as root with ``HOME`` unset. The
test renders it the same way in Python and runs the result with ``/bin/bash``. ``getent``,
``sudo``, ``git``, ``sleep`` and ``id`` are fakes on ``PATH``, and ``chmod`` and ``rm`` log
before they run the real tool. A clone writes a checkout whose ``deploy/vm-bootstrap.sh``
logs how it was run, so a test reads back whether the shim handed over and with what.

The shim writes ``/etc/marketlake`` by its absolute path and takes no install root, so the
test rewrites that literal in the rendered text to a directory under the test's own
temporary root. That rewrite is the price of testing a template that is not
prefix-aware: the script that runs is the rendered shim with one path changed, not the
exact bytes cloud-init receives. ``test_the_rewrite_leaves_no_system_path`` checks that
no other absolute path survives the rewrite. The owner's home needs no rewrite, because
the shim reads it from ``getent``, whose fake answers a temporary directory.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

from tests.support.fake_disk import FAKE_CHMOD, FAKE_RM, FAKE_SLEEP, NEXT_RC
from tests.support.fake_systemd import install_fakes

REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE = REPO_ROOT / "infra" / "live" / "user-data.sh.tftpl"

OWNER = "someone"
VOLUME_ID = "vol-0fedcba9876543210"
REPO_URL = "https://github.com/l3a0/marketlake.git"
CONF_DIR = "/etc/marketlake"

# git as the shim calls it. A clone fails or succeeds in the order CLONE_RCS lists. A
# failed clone leaves a directory with no HEAD behind, as one cut off part way does, and
# a clone into a directory that is not empty fails, as git's does. A successful clone
# writes a HEAD and links the checkout's vm-bootstrap.sh to the shared fake.
FAKE_GIT = (
    "#!/bin/bash\n"
    'printf \'git %s\\n\' "$*" >> "$LOG"\n'
    + NEXT_RC
    + r"""if [[ "$1" == "-C" && "$3" == "rev-parse" ]]; then
  if [[ ! -d "$2/.git" ]]; then
    echo "fatal: not a git repository (or any of the parent directories): .git" >&2
    exit 128
  fi
  [[ -f "$2/.git/HEAD" ]] && exit 0
  exit 1
fi
if [[ "$1" == "clone" ]]; then
  dest="${*: -1}"
  if [[ -e "$dest" ]] && [[ -n "$(ls -A "$dest")" ]]; then
    echo "fatal: destination path '$dest' already exists and is not an empty directory." >&2
    exit 128
  fi
  rc="$(next_rc clone "${CLONE_RCS:-}")"
  mkdir -p "$dest/.git"
  if [[ "$rc" != 0 ]]; then
    echo "fatal: unable to access '${*: -2:1}': Could not resolve host: github.com" >&2
    exit "$rc"
  fi
  echo "ref: refs/heads/main" > "$dest/.git/HEAD"
  mkdir -p "$dest/deploy"
  ln -s "$TOOLS/vm-bootstrap.sh" "$dest/deploy/vm-bootstrap.sh"
  exit 0
fi
echo "fake git: unexpected arguments $*" >&2
exit 9
"""
)

# The checkout's bootstrap. It logs its argv and keeps its pid, the path it was run by
# and its environment, so a test can tell an exec from a call and see what it inherited.
FAKE_BOOTSTRAP = r"""#!/bin/bash
line="vm-bootstrap $*"
printf '%s\n' "${line% }" >> "$LOG"
printf '%s' "$$" > "$STATE/bootstrap-pid"
printf '%s' "$0" > "$STATE/bootstrap-path"
env > "$STATE/bootstrap-env"
exit 0
"""


def render(template: str, variables: dict[str, str]) -> str:
    """Render ``template`` as OpenTofu's ``templatefile`` would.

    ``${name}`` takes the variable's value, ``$${`` is a literal ``${`` and ``%%{`` a
    literal ``%{``. ``templatefile`` refuses a name it was not given, and so does this. A
    ``%{`` directive is refused too, since this renderer does not evaluate one.
    """

    def substitute(match: re.Match[str]) -> str:
        token = match.group(0)
        if token == "$${":
            return "${"
        if token == "%%{":
            return "%{"
        if token == "%{":
            raise ValueError("the template uses a directive this renderer cannot evaluate")
        name = match.group(1).strip()
        if name not in variables:
            raise KeyError(f"the template names {name!r}, which it was not given")
        return variables[name]

    return re.sub(r"\$\$\{|%%\{|\$\{([^}]*)\}|%\{", substitute, template)


def rendered_shim(owner: str = OWNER, volume_id: str = VOLUME_ID) -> str:
    return render(TEMPLATE.read_text(), {"owner": owner, "lake_volume_id": volume_id})


class Host:
    """A fake first boot: a temporary root for ``/etc/marketlake`` and the owner's home.

    The owner exists with no checkout, and every clone succeeds. A test changes what it
    needs before it runs the shim.
    """

    def __init__(self, tmp_path: Path, tools: Path) -> None:
        self.root = tmp_path / "root"
        self.state = tmp_path / "state"
        self.home = tmp_path / "home"
        self.log = tmp_path / "log"
        for directory in (self.root, self.state, self.home):
            directory.mkdir()
        self.log.write_text("")
        self.conf_dir = self.root / CONF_DIR.lstrip("/")
        self.script = tmp_path / "user-data.sh"
        self.script.write_text(rendered_shim().replace(CONF_DIR, str(self.conf_dir)))
        self.cwd = tmp_path
        # cloud-init runs the shim with HOME unset, so the environment carries none.
        self.env = {
            "PATH": f"{tools / 'bin'}:/usr/bin:/bin",
            "LOG": str(self.log),
            "STATE": str(self.state),
            "TOOLS": str(tools),
            "FAKE_OWNER": OWNER,
            "FAKE_HOME": str(self.home),
        }

    @property
    def conf(self) -> Path:
        return self.conf_dir / "bootstrap.conf"

    @property
    def checkout(self) -> Path:
        return self.home / "marketlake"

    def run(self, umask: int = 0o022, **env: str) -> tuple[subprocess.CompletedProcess[str], int]:
        """Run the shim under ``umask``, returning its result and the pid of its bash."""
        proc = subprocess.Popen(
            ["/bin/bash", str(self.script)],
            env={**self.env, **env},
            cwd=self.cwd,
            umask=umask,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout, stderr = proc.communicate(timeout=60)
        result = subprocess.CompletedProcess(proc.args, proc.returncode, stdout, stderr)
        return result, proc.pid

    def calls(self) -> list[str]:
        return [line for line in self.log.read_text().splitlines() if line]

    def ran(self, prefix: str) -> list[str]:
        return [line for line in self.calls() if line.startswith(prefix)]

    def clone_line(self) -> str:
        return f"sudo -u {OWNER} -H git clone --quiet --branch main {REPO_URL} {self.checkout}"

    def valid_checkout(self) -> None:
        """A checkout a finished clone left, with a HEAD and the bootstrap in place."""
        (self.checkout / ".git").mkdir(parents=True)
        (self.checkout / ".git" / "HEAD").write_text("ref: refs/heads/main\n")
        (self.checkout / "deploy").mkdir()
        (self.checkout / "deploy" / "vm-bootstrap.sh").symlink_to(
            Path(self.env["TOOLS"]) / "vm-bootstrap.sh"
        )


def _executable(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    path.chmod(0o755)


def _shim_lines(proc: subprocess.CompletedProcess[str]) -> list[str]:
    """The shim's own lines on stderr. The fake git prints its own errors too."""
    return [line for line in proc.stderr.splitlines() if line.startswith("user-data:")]


@pytest.fixture(scope="module")
def tools(tmp_path_factory) -> Path:
    """Every executable the tests run, written once for the module.

    A Mac checks each new executable file on its first run, so the clone links each
    checkout's ``vm-bootstrap.sh`` to the one here rather than writing a fresh one.
    """
    shared = tmp_path_factory.mktemp("shim-tools")
    install_fakes(shared / "bin")
    for name, body in {
        "git": FAKE_GIT,
        "chmod": FAKE_CHMOD,
        "rm": FAKE_RM,
        "sleep": FAKE_SLEEP,
    }.items():
        _executable(shared / "bin" / name, body)
    _executable(shared / "vm-bootstrap.sh", FAKE_BOOTSTRAP)
    return shared


@pytest.fixture
def host(tmp_path, tools) -> Host:
    return Host(tmp_path, tools)


def _assert_handed_over(host: Host, proc: subprocess.CompletedProcess[str], pid: int) -> None:
    """The shim exec'd the checkout's bootstrap with no arguments and no HOME."""
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert host.ran("vm-bootstrap") == ["vm-bootstrap"], host.calls()
    script = host.checkout / "deploy" / "vm-bootstrap.sh"
    assert (host.state / "bootstrap-path").read_text() == str(script)
    # exec replaces the shim's bash, so the bootstrap runs in the same process.
    assert (host.state / "bootstrap-pid").read_text() == str(pid)
    env = (host.state / "bootstrap-env").read_text().splitlines()
    assert not [line for line in env if line.startswith("HOME=")], env


# -- the render ------------------------------------------------------------------------


def test_the_render_leaves_no_placeholder():
    text = rendered_shim()
    assert not re.search(r"\$\{\s*(owner|lake_volume_id)\s*\}", text), text
    assert f"OWNER={OWNER}\nLAKE_VOLUME_ID={VOLUME_ID}\n" in text


def test_the_renderer_follows_templatefile_escapes():
    assert render("$${HOME} %%{ x } ${ a }", {"a": "1"}) == "${HOME} %{ x } 1"
    with pytest.raises(KeyError):
        render("${b}", {"a": "1"})
    with pytest.raises(ValueError):
        render("%{ if a }x%{ endif }", {"a": "1"})


def test_the_rewrite_leaves_no_system_path(host):
    """Every absolute path the rendered shim names is one the test moves or one it only
    reads, so a run cannot write outside the test's temporary root."""
    paths = set(re.findall(r"(?<![\w.:/$])/[\w./-]+", rendered_shim()))
    assert paths == {"/bin/bash", CONF_DIR, f"{CONF_DIR}/bootstrap.conf", "/dev/null"}
    text = host.script.read_text()
    assert CONF_DIR not in text.replace(str(host.conf_dir), "")


def test_the_shim_carries_only_the_owner_and_the_volume_id():
    """``user_data`` is readable by anyone who can describe the instance, so the render
    adds nothing beyond the two values it was given."""
    text = rendered_shim()
    assert set(re.findall(r"vol-[0-9a-f]+", text)) == {VOLUME_ID}
    assert not re.search(r"\b\d{1,3}(\.\d{1,3}){3}\b", text), "an IPv4 address"
    assert not re.search(r"\b[0-9a-f]{1,4}(:[0-9a-f]{0,4}){2,7}\b", text, re.I), "an IPv6"
    assert not re.search(r"\d{12}", text), "a 12-digit number, such as an account id"
    # Each value appears only where the template put it.
    template = TEMPLATE.read_text()
    assert text.count(OWNER) == template.count("${owner}")
    assert text.count(VOLUME_ID) == template.count("${lake_volume_id}")


# -- bootstrap.conf --------------------------------------------------------------------


def test_bootstrap_conf_holds_the_two_lines_before_the_clone(host):
    # cloud-init's umask is not promised, so the modes must not depend on it.
    proc, pid = host.run(umask=0o077)
    _assert_handed_over(host, proc, pid)
    assert host.conf.read_text() == f"OWNER={OWNER}\nLAKE_VOLUME_ID={VOLUME_ID}\n"
    assert host.conf.stat().st_mode & 0o777 == 0o644
    assert host.conf_dir.stat().st_mode & 0o777 == 0o755
    calls = host.calls()
    chmod = calls.index(f"chmod 0644 {host.conf}")
    assert chmod < calls.index(host.clone_line()), calls


def test_bootstrap_conf_is_rewritten_over_a_loose_one(host):
    """A rerun by hand finds the file from the first boot and replaces it whole."""
    host.conf_dir.mkdir(parents=True)
    host.conf.write_text("OWNER=other\nLAKE_VOLUME_ID=vol-0aaaaaaaaaaaaaaaa\nEXTRA=1\n")
    host.conf.chmod(0o666)
    proc, pid = host.run()
    _assert_handed_over(host, proc, pid)
    assert host.conf.read_text() == f"OWNER={OWNER}\nLAKE_VOLUME_ID={VOLUME_ID}\n"
    assert host.conf.stat().st_mode & 0o777 == 0o644


def test_a_conf_directory_that_cannot_be_made_stops_before_the_clone(host):
    """A file where the directory goes makes the first step fail, so the shim stops there."""
    host.conf_dir.parent.mkdir(parents=True)
    host.conf_dir.write_text("not a directory\n")
    proc, _ = host.run()
    assert proc.returncode != 0, proc.stdout + proc.stderr
    assert not host.ran("sudo"), host.calls()
    assert not host.ran("vm-bootstrap"), host.calls()
    assert host.conf_dir.read_text() == "not a directory\n"


# -- the clone -------------------------------------------------------------------------


def test_a_missing_checkout_is_cloned_as_the_owner_then_handed_over(host):
    proc, pid = host.run()
    _assert_handed_over(host, proc, pid)
    calls = host.calls()
    assert host.ran("sudo") == [host.clone_line()], calls
    assert not host.ran("git -C"), calls
    assert calls.index(host.clone_line()) < calls.index("vm-bootstrap"), calls


def test_a_clone_that_fails_twice_is_retried(host):
    proc, pid = host.run(CLONE_RCS="128 128 0")
    _assert_handed_over(host, proc, pid)
    calls = host.calls()
    assert host.ran("sudo") == [host.clone_line()] * 3, calls
    assert host.ran("sleep") == ["sleep 30"] * 2, calls
    # Each failed attempt's leftovers go before the next attempt.
    assert host.ran("rm") == [f"rm -rf -- {host.checkout}"] * 2, calls
    assert not _shim_lines(proc), proc.stderr


def test_a_clone_that_always_fails_stops_after_ten(host):
    proc, _ = host.run(CLONE_RCS="128")
    assert proc.returncode != 0, proc.stdout + proc.stderr
    calls = host.calls()
    assert host.ran("sudo") == [host.clone_line()] * 10, calls
    assert host.ran("sleep") == ["sleep 30"] * 9, calls
    assert _shim_lines(proc) == [
        "user-data: cloning marketlake failed 10 times, so no bootstrap ran"
    ], proc.stderr
    assert not host.ran("vm-bootstrap"), calls
    assert not (host.state / "bootstrap-pid").exists()


def test_a_checkout_with_no_head_is_removed_and_cloned_again(host):
    """A clone cut off by a reboot leaves a directory git cannot read a HEAD from."""
    (host.checkout / ".git").mkdir(parents=True)
    (host.checkout / "stale").write_text("left by a cut-off clone\n")
    proc, pid = host.run()
    _assert_handed_over(host, proc, pid)
    calls = host.calls()
    verify = f"sudo -u {OWNER} -H git -C {host.checkout} rev-parse --verify --quiet HEAD"
    remove = f"rm -rf -- {host.checkout}"
    assert host.ran("sudo") == [verify, host.clone_line()], calls
    assert calls.index(verify) < calls.index(remove) < calls.index(host.clone_line())
    assert not (host.checkout / "stale").exists()


def test_a_directory_that_is_no_repository_is_cloned_again(host):
    host.checkout.mkdir()
    (host.checkout / "stale").write_text("not a repository\n")
    proc, pid = host.run()
    _assert_handed_over(host, proc, pid)
    assert host.ran("rm") == [f"rm -rf -- {host.checkout}"], host.calls()
    assert host.ran("sudo")[-1] == host.clone_line()


def test_a_valid_checkout_is_not_cloned_again(host):
    host.valid_checkout()
    proc, pid = host.run()
    _assert_handed_over(host, proc, pid)
    calls = host.calls()
    verify = f"sudo -u {OWNER} -H git -C {host.checkout} rev-parse --verify --quiet HEAD"
    assert host.ran("sudo") == [verify], calls
    assert not host.ran("rm"), calls
    assert not host.ran("sleep"), calls


def test_a_missing_owner_stops_with_one_line(host):
    proc, _ = host.run(FAKE_OWNER="nobody-else")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert _shim_lines(proc) == [f"user-data: no account named {OWNER}"], proc.stderr
    assert not host.ran("sudo"), host.calls()
    assert not host.ran("vm-bootstrap"), host.calls()
