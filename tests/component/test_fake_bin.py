"""The one program behind every fake, and the tools that use it.

``tests.support.fake_bin`` installs each fake as a symlink to one dispatcher script, which
sources the fake's body. A Mac then scans one new file per process rather than one per
fake. Two things can undo that quietly. A fake written later as its own file pays the scan
again with nothing failing, and a dispatcher that changes the shell it hands a body would
change what every fake does. The first half of this file checks every tool layout the
shared installers and the VM fixtures build. The second runs the dispatcher itself.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from tests.component import test_control_plane_systemd as systemd
from tests.component import test_vm_bootstrap as bootstrap
from tests.component import test_vm_shim as shim
from tests.support.fake_bin import dispatcher, install
from tests.support.fake_disk import install_disk_fakes
from tests.support.fake_systemd import FAKES, install_fakes

REPO_ROOT = Path(__file__).resolve().parents[2]


def _tracked_scripts() -> set[Path]:
    """The repository's executable files under ``deploy``, as git records them."""
    staged = subprocess.run(
        ["git", "ls-files", "-s", "deploy"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    scripts = set()
    for line in staged.splitlines():
        meta, name = line.split("\t")
        if meta.split()[0] == "100755":
            scripts.add((REPO_ROOT / name).resolve())
    return scripts


def _programs(root: Path) -> dict[str, Path]:
    """Every link and executable file under ``root``, by path relative to it, resolved.

    A link must end at the dispatcher or at a tracked script, and no other file may be
    executable, so a fake written as its own file fails here.
    """
    allowed = {dispatcher().resolve(), *_tracked_scripts()}
    found = {}
    for directory, dirnames, filenames in os.walk(root):
        for name in dirnames + filenames:
            path = Path(directory) / name
            relative = str(path.relative_to(root))
            if path.is_symlink():
                target = path.resolve()
                assert target in allowed, f"{relative} links to {target}"
                found[relative] = target
            elif path.is_file():
                assert not os.access(path, os.X_OK), f"{relative} is an executable file"
    return found


def test_the_shared_installers_write_only_links_to_the_dispatcher(tmp_path):
    install_fakes(tmp_path / "systemd")
    install_disk_fakes(tmp_path / "disk")
    found = _programs(tmp_path)
    assert set(found.values()) == {dispatcher().resolve()}
    systemd_names = {key.split("/", 1)[1] for key in found if key.startswith("systemd/")}
    disk_names = {key.split("/", 1)[1] for key in found if key.startswith("disk/")}
    assert systemd_names == set(FAKES)
    assert disk_names > set(FAKES) | {"blkid", "mkfs.ext4", "dd", "findmnt", "python3"}


def test_the_bootstrap_tools_and_checkout_are_links(tmp_path):
    tools = bootstrap.build_tools(tmp_path / "tools")
    (tmp_path / "vm").mkdir()
    vm = bootstrap.VM(tmp_path / "vm", tools)
    vm.set_uv("0.10.0")
    found = _programs(tmp_path)
    assert found["tools/vm-bootstrap.sh"] == bootstrap.BOOTSTRAP.resolve()
    assert found["tools/vm-empty-shadow-lake.sh"] == bootstrap.EMPTY.resolve()
    assert found["vm/checkout/deploy/vm-bootstrap.sh"] == bootstrap.BOOTSTRAP.resolve()
    assert found["vm/checkout/deploy/linux-install.sh"] == dispatcher().resolve()
    assert found["vm/checkout/.venv/bin/python"] == dispatcher().resolve()
    assert found["vm/home/.local/bin/uv"] == dispatcher().resolve()
    assert "tools/uv-0.10.0" in found


def test_the_shim_tools_and_checkout_are_links(tmp_path):
    tools = shim.build_tools(tmp_path / "tools")
    (tmp_path / "host").mkdir()
    host = shim.Host(tmp_path / "host", tools)
    host.valid_checkout()
    found = _programs(tmp_path)
    for name in ("bin/git", "bin/sleep", "bin/sudo", "vm-bootstrap.sh"):
        assert found[f"tools/{name}"] == dispatcher().resolve(), name
    assert found["host/home/marketlake/deploy/vm-bootstrap.sh"] == dispatcher().resolve()


def test_the_entry_point_checkout_and_harness_are_links(tmp_path):
    systemd._checkout(tmp_path)
    systemd.Harness(tmp_path)
    found = _programs(tmp_path)
    assert found["checkout/deploy/linux-install.sh"] == systemd.ENTRY_POINT.resolve()
    assert found["checkout/.venv/bin/python"] == dispatcher().resolve()
    assert found["home/.local/bin/uv"] == dispatcher().resolve()
    assert found["bin/systemctl"] == dispatcher().resolve()


# -- the dispatcher --------------------------------------------------------------------


def _run(argv: list[str | Path], **kwargs) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(arg) for arg in argv],
        env={"PATH": "/usr/bin:/bin", "KEEP": "kept"},
        capture_output=True,
        text=True,
        timeout=60,
        **kwargs,
    )


def test_the_arguments_and_the_link_path_reach_the_body(tmp_path):
    fake = tmp_path / "bin" / "echoer"
    install(fake, '#!/bin/bash\nprintf \'%s\\n\' "$0" "$#" "$@"\n')
    proc = _run([fake, "a b", "", "-e", "*"])
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.splitlines() == [str(fake), "4", "a b", "", "-e", "*"]


def test_the_body_runs_with_errexit_and_nounset_off(tmp_path):
    # A failing command and an unset $1 both carry on, as the bodies rely on.
    fake = tmp_path / "bin" / "flags"
    install(fake, '#!/bin/bash\nfalse\nprintf \'%s\\n\' "$-" "[$1]"\nexit 3\n')
    proc = _run([fake])
    assert proc.returncode == 3, proc.stderr
    flags, first = proc.stdout.splitlines()
    assert "e" not in flags and "u" not in flags, flags
    assert first == "[]"


def test_a_chained_link_finds_its_body_a_hop_away(tmp_path):
    shared = tmp_path / "tools" / "python"
    install(shared, "#!/bin/bash\nprintf '%s\\n' \"$0\"\n")
    absolute = tmp_path / "venv" / "bin" / "python"
    absolute.parent.mkdir(parents=True)
    absolute.symlink_to(shared)
    relative = tmp_path / "other" / "python"
    relative.parent.mkdir()
    relative.symlink_to(Path("..") / "venv" / "bin" / "python")
    for link in (absolute, relative):
        proc = _run([link])
        assert proc.returncode == 0, proc.stderr
        assert proc.stdout == f"{link}\n"


def test_a_missing_body_exits_127_naming_the_path(tmp_path):
    ghost = tmp_path / "bin" / "ghost"
    install(ghost, "#!/bin/bash\nexit 0\n")
    (tmp_path / "bin" / ".fake" / "ghost").unlink()
    proc = _run([ghost, "x"])
    assert proc.returncode == 127
    assert proc.stdout == ""
    assert proc.stderr == f"fake: no body for {ghost}\n"


def test_the_body_inherits_the_environment_unchanged(tmp_path):
    # The same body run as /bin/bash <file> is the baseline: nothing the dispatcher sets
    # may reach the body's environment.
    body = "#!/bin/bash\nenv\n"
    fake = tmp_path / "bin" / "env-dump"
    install(fake, body)
    plain = tmp_path / "plain"
    plain.write_text(body)
    through = _run([fake]).stdout.splitlines()
    baseline = _run(["/bin/bash", plain]).stdout.splitlines()
    assert "KEEP=kept" in through
    assert sorted(through) == sorted(baseline)


def test_descriptor_9_reaches_the_body_as_it_was(tmp_path):
    fake = tmp_path / "bin" / "fd9"
    install(fake, "#!/bin/bash\nif { : >&9; } 2>/dev/null; then echo open; else echo shut; fi\n")
    assert _run([fake]).stdout == "shut\n"
    # bash opens descriptor 9 and execs the fake, as the scripts hold the install lock.
    held = _run(["/bin/bash", "-c", 'exec 9>"$1"; exec "$0"', fake, tmp_path / "lock"])
    assert held.stdout == "open\n", held.stderr


def test_install_replaces_a_file_or_an_earlier_fake(tmp_path):
    fake = tmp_path / "bin" / "sleep"
    fake.parent.mkdir()
    fake.write_text("#!/bin/bash\necho file\n")
    install(fake, "#!/bin/bash\necho first\n")
    assert _run([fake]).stdout == "first\n"
    install(fake, "#!/bin/bash\necho second\n")
    assert _run([fake]).stdout == "second\n"


@pytest.mark.skipif(os.geteuid() == 0, reason="root writes through a read-only mode")
def test_a_write_through_a_link_raises(tmp_path):
    fake = tmp_path / "bin" / "sleep"
    install(fake, "#!/bin/bash\nexit 0\n")
    before = dispatcher().read_text()
    with pytest.raises(PermissionError):
        fake.write_text("#!/bin/bash\nexit 1\n")
    assert dispatcher().read_text() == before
