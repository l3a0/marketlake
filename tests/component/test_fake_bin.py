"""The one program behind every fake, and the tools that use it.

``tests.support.fake_bin`` installs each fake as a symlink to one dispatcher script, which
sources the fake's body. A Mac then scans one new file per process rather than one per
fake. Two things can undo that quietly. A fake written later as its own file pays the scan
again with nothing failing, and a dispatcher that changes the shell it hands a body would
change what every fake does. The first part of this file checks the layouts the shared
installers and the entry point's checkout build. The VM tests check their own layouts,
from the fixtures they run with. The rest runs the dispatcher itself.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.component import test_control_plane_systemd as systemd
from tests.support.fake_bin import checked_links, dispatcher, install
from tests.support.fake_disk import install_disk_fakes
from tests.support.fake_systemd import FAKES, install_fakes

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_the_shared_installers_write_only_links_to_the_dispatcher(tmp_path):
    install_fakes(tmp_path / "systemd")
    install_disk_fakes(tmp_path / "disk")
    found = checked_links(tmp_path)
    assert set(found.values()) == {dispatcher().resolve()}
    systemd_names = {key.split("/", 1)[1] for key in found if key.startswith("systemd/")}
    disk_names = {key.split("/", 1)[1] for key in found if key.startswith("disk/")}
    assert systemd_names == set(FAKES)
    assert disk_names > set(FAKES) | {"blkid", "mkfs.ext4", "dd", "findmnt", "python3"}


def test_the_entry_point_checkout_and_harness_are_links(tmp_path):
    systemd._checkout(tmp_path)
    systemd.Harness(tmp_path)
    found = checked_links(tmp_path, systemd.ENTRY_POINT)
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
    # A link named apart from its target, as the owner's uv links to tools/uv-0.10.0. Its
    # body is found by the name of the hop that reached it, not by the name it was run as.
    versioned = tmp_path / "tools" / "uv-0.10.0"
    install(versioned, "#!/bin/bash\nprintf '%s\\n' \"$0\"\n")
    renamed = tmp_path / "home" / "uv"
    renamed.parent.mkdir()
    renamed.symlink_to(versioned)
    for link in (absolute, relative, renamed):
        proc = _run([link])
        assert proc.returncode == 0, proc.stderr
        assert proc.stdout == f"{link}\n"


def test_a_slash_free_path_finds_its_body_in_the_working_directory(tmp_path):
    # bash given a bare name reads that file from the working directory, so $0 holds no
    # directory at all.
    fake = tmp_path / "bin" / "echoer"
    install(fake, "#!/bin/bash\nprintf '%s\\n' \"$0\"\n")
    proc = _run(["/bin/bash", "echoer"], cwd=fake.parent)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout == "echoer\n"


def test_a_chain_is_followed_for_eight_hops_and_no_further(tmp_path):
    shared = tmp_path / "tools" / "python"
    install(shared, "#!/bin/bash\necho ran\n")
    links, previous = [], shared
    for n in range(9):
        link = tmp_path / f"hop{n}" / "python"
        link.parent.mkdir()
        link.symlink_to(previous)
        links.append(link)
        previous = link
    # links[k] is k + 1 hops from the body.
    near = _run([links[7]])
    assert (near.returncode, near.stdout) == (0, "ran\n"), near.stderr
    far = _run([links[8]])
    assert far.returncode == 127
    assert far.stderr == f"fake: no body for {links[8]}\n"


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


def test_the_body_sees_the_shell_state_bash_gives_a_script(tmp_path):
    # Beyond the environment: umask, options, shopts, traps and the working directory
    # match a plain bash run, and the one variable left behind is the body's path.
    body = (
        '#!/bin/bash\numask\necho "$-"\nshopt -p\nset -o\ntrap -p\npwd\n'
        "compgen -v | grep '^__fake_' || true\n"
    )
    fake = tmp_path / "bin" / "state"
    install(fake, body)
    plain = tmp_path / "plain"
    plain.write_text(body)
    through = _run([fake]).stdout.splitlines()
    baseline = _run(["/bin/bash", plain]).stdout.splitlines()
    assert through == [*baseline, "__fake_body"]


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


def test_the_dispatcher_outlives_a_forked_child_and_goes_at_exit(tmp_path):
    # A forked child runs the exit hook it inherited, which must leave the dispatcher in
    # place for its parent. The parent's own exit removes it.
    script = (
        "import os, sys\n"
        "from tests.support.fake_bin import dispatcher\n"
        "path = dispatcher()\n"
        "child = os.fork()\n"
        "if child == 0:\n"
        "    sys.exit(0)\n"
        "os.waitpid(child, 0)\n"
        "print(path, path.exists())\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        cwd=REPO_ROOT,
        env={**os.environ, "TMPDIR": str(tmp_path)},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    path, alive = proc.stdout.split()
    assert alive == "True"
    assert not Path(path).exists()
