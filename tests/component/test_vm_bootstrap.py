"""The hosted VM's two root scripts, run for real against fakes.

``deploy/vm-bootstrap.sh`` takes a fresh VM from cloud-init to a running daemon, and
``deploy/vm-empty-shadow-lake.sh`` empties a shadow's lake before a restore. Both run here
under ``/bin/bash`` with ``MARKETLAKE_INSTALL_ROOT`` pointed at a temporary directory and
every tool they call replaced by a fake from ``tests.support.fake_disk`` and
``tests.support.fake_systemd``. The fake disk keeps its state in a directory, so a test
sets up a fresh volume, an existing filesystem or a mounted one, and reads back what the
script did to it. ``deploy/linux-install.sh`` is a fake here too, since
``test_control_plane_systemd.py`` runs the real one.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from lake import control_plane as cp
from tests.support.fake_disk import (
    FAKE_LINUX_INSTALL,
    FAKE_UUID,
    FAKE_VENV_PYTHON,
    install_disk_fakes,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP = REPO_ROOT / "deploy" / "vm-bootstrap.sh"
EMPTY = REPO_ROOT / "deploy" / "vm-empty-shadow-lake.sh"

OWNER = "someone"
VOLUME_ID = "vol-0123456789abcdef0"
# The by-id link udev makes for that volume: the NVMe model, then the id without its dash.
DEVICE_NAME = "nvme-Amazon_Elastic_Block_Store_vol0123456789abcdef0"
LAKE_ROOT = "/srv/marketlake"
UNIT = "srv-marketlake.mount"
NEW_UUID = "11111111-2222-3333-4444-555555555555"
OTHER_UUID = "99999999-8888-7777-6666-555555555555"
UV_VERSION = "0.11.2"
ROOT_LINE = "LABEL=cloudimg-rootfs / ext4 discard,errors=remount-ro 0 1\n"
VM_YAML = (
    "role: shadow\n"
    f"lake_root: {LAKE_ROOT}\n"
    "token_store: store\n"
    "bucket_credentials: instance_profile\n"
    "bucket_region: us-east-1\n"
)


def _lake_line(uuid: str) -> str:
    return f"UUID={uuid} {LAKE_ROOT} ext4 defaults,nofail 0 2\n"


class VM:
    """A fake VM: a checkout, the owner's home, an install root and a fake lake volume.

    The volume starts fresh, with no filesystem, and unmounted, the owner has the pinned
    ``uv``, and every Python step succeeds. A test changes what it needs before it runs a script.

    Every executable is a symlink into ``tools``, which the module builds once. A Mac
    checks each new executable file on its first run, at about 0.2 seconds a file, so
    fresh fakes for each test cost several seconds a run. The scripts find their checkout
    from the path they were run by, which is the symlink's, so each test still gets its
    own checkout.
    """

    def __init__(self, tmp_path: Path, tools: Path) -> None:
        self.tmp = tmp_path
        self.tools = tools
        self.root = tmp_path / "root"
        self.state = tmp_path / "state"
        self.log = tmp_path / "log"
        self.checkout = tmp_path / "checkout"
        self.home = tmp_path / "home"
        self.temp = tmp_path / "tmp"
        for directory in (self.root, self.state, self.temp):
            directory.mkdir()
        self.log.write_text("")
        self.env = {
            "PATH": f"{tools / 'bin'}:/usr/bin:/bin",
            "LOG": str(self.log),
            "STATE": str(self.state),
            "FAKE_OWNER": OWNER,
            "FAKE_HOME": str(self.home),
            "TMPDIR": str(self.temp),
            cp.INSTALL_ROOT_ENV: str(self.root),
            cp.INSTALL_TEST_ENV: "1",
        }
        deploy = self.checkout / "deploy"
        deploy.mkdir(parents=True)
        for name in (BOOTSTRAP.name, EMPTY.name, "linux-install.sh"):
            (deploy / name).symlink_to(tools / name)
        venv = self.checkout / ".venv" / "bin"
        venv.mkdir(parents=True)
        (venv / "python").symlink_to(tools / "python")
        (self.checkout / "config").mkdir()
        (self.checkout / "config" / "vm.yaml").write_text(VM_YAML)
        shutil.copy2(REPO_ROOT / "config" / "tickers.yaml", self.checkout / "config")
        (self.checkout / ".tool-versions").write_text(f"uv {UV_VERSION}\n")
        self.set_uv(UV_VERSION)
        self.conf.parent.mkdir(parents=True)
        self.conf.write_text(f"OWNER={OWNER}\nLAKE_VOLUME_ID={VOLUME_ID}\n")
        self.fstab.write_text(ROOT_LINE)
        self.device.parent.mkdir(parents=True)
        self.device.write_text("")
        self.disk.mkdir()

    def run(self, argv: list[str], **env: str) -> subprocess.CompletedProcess[str]:
        self.log.write_text("")
        return subprocess.run(
            argv, env={**self.env, **env}, capture_output=True, text=True, timeout=60
        )

    def calls(self) -> list[str]:
        return [line for line in self.log.read_text().splitlines() if line]

    # -- paths -------------------------------------------------------------------------

    @property
    def conf(self) -> Path:
        return self.root / "etc" / "marketlake" / "bootstrap.conf"

    @property
    def fstab(self) -> Path:
        return self.root / "etc" / "fstab"

    @property
    def device(self) -> Path:
        return self.root / "dev" / "disk" / "by-id" / DEVICE_NAME

    @property
    def lake(self) -> Path:
        return self.root / LAKE_ROOT.lstrip("/")

    @property
    def disk(self) -> Path:
        return self.state / "disk"

    @property
    def lock(self) -> Path:
        return self.root / cp.INSTALL_LOCK.lstrip("/")

    # -- state -------------------------------------------------------------------------

    def set_uv(self, version: str | None) -> None:
        """Give the owner a ``uv`` that reports this version, or none at all."""
        uv = self.home / ".local" / "bin" / "uv"
        uv.unlink(missing_ok=True)
        if version is None:
            return
        shared = self.tools / f"uv-{version}"
        if not shared.exists():
            _executable(shared, f'#!/bin/bash\necho "uv {version} (abc123 2026-01-01)"\n')
        uv.parent.mkdir(parents=True, exist_ok=True)
        uv.symlink_to(shared)

    def ext4(self, uuid: str = FAKE_UUID) -> None:
        """Give the volume an ext4 filesystem, as a replacement instance finds it."""
        self.disk.joinpath("probe").write_text(f"UUID={uuid}\nTYPE=ext4\n")
        self.disk.joinpath("uuid").write_text(uuid)

    def mount(self, uuid: str = FAKE_UUID) -> None:
        """Mount a filesystem with this UUID at the lake root, with its directory."""
        self.lake.mkdir(parents=True, exist_ok=True)
        (self.state / "mounted").write_text(uuid)

    def immutable(self) -> list[str]:
        path = self.state / "immutable"
        return path.read_text().splitlines() if path.exists() else []

    def bootstrap(self, **env: str) -> subprocess.CompletedProcess[str]:
        return self.run([str(self.checkout / "deploy" / BOOTSTRAP.name)], **env)

    def empty(self, **env: str) -> subprocess.CompletedProcess[str]:
        return self.run([str(self.checkout / "deploy" / EMPTY.name)], **env)

    def ran(self, prefix: str) -> list[str]:
        return [line for line in self.calls() if line.startswith(prefix)]

    def index(self, line: str) -> int:
        return self.calls().index(line)


def _executable(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    path.chmod(0o755)


def _one_line(proc: subprocess.CompletedProcess[str], prefix: str) -> str:
    """The single line of its own a stopped run printed on stderr.

    A failing tool prints its own message too, as ``systemctl start`` does, so only the
    script's lines are counted.
    """
    lines = [line for line in proc.stderr.splitlines() if line.startswith(prefix)]
    assert len(lines) == 1, proc.stdout + proc.stderr
    return lines[0]


def _in_order(calls: list[str], steps: list[str]) -> bool:
    """Whether every step appears in ``calls``, each after the one before it."""
    position = 0
    for step in steps:
        try:
            position = calls.index(step, position) + 1
        except ValueError:
            return False
    return True


def _assert_nothing_installed(vm: VM, *, chowned: bool = False) -> None:
    """The run stopped before the install, and before the owner's chown unless it ran."""
    calls = vm.calls()
    assert not vm.ran("linux-install"), calls
    assert not vm.ran("flock"), calls
    assert not vm.ran("venv-python"), calls
    assert bool(vm.ran(f"chown {OWNER}:")) == chowned, calls


@pytest.fixture(scope="module")
def tools(tmp_path_factory) -> Path:
    """Every executable the tests run, written once for the module."""
    shared = tmp_path_factory.mktemp("vm-tools")
    install_disk_fakes(shared / "bin")
    for script in (BOOTSTRAP, EMPTY):
        shutil.copy2(script, shared / script.name)
    _executable(shared / "linux-install.sh", FAKE_LINUX_INSTALL)
    _executable(shared / "python", FAKE_VENV_PYTHON)
    return shared


@pytest.fixture
def vm(tmp_path, tools) -> VM:
    return VM(tmp_path, tools)


# -- tracking --------------------------------------------------------------------------


@pytest.mark.parametrize("script", [BOOTSTRAP, EMPTY], ids=lambda path: path.name)
def test_the_script_is_tracked_executable(script):
    staged = subprocess.run(
        ["git", "ls-files", "-s", str(script.relative_to(REPO_ROOT))],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert staged.split()[0] == "100755", staged
    assert os.access(script, os.X_OK)


def test_ci_and_the_bootstrap_read_one_uv_pin():
    assert (REPO_ROOT / ".tool-versions").read_text() == f"uv {UV_VERSION}\n"
    ci = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text()
    setup = ci[ci.index("uses: astral-sh/setup-uv@") :]
    setup = setup[: setup.index("\n      - ")]
    assert 'version-file: ".tool-versions"' in setup
    assert "enable-cache: true" in setup
    assert "\n          version:" not in setup
    assert UV_VERSION not in ci


# -- the first boot --------------------------------------------------------------------


def test_a_first_boot_formats_a_fresh_volume_and_installs(vm):
    proc = vm.bootstrap()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert proc.stdout.splitlines()[-1] == "vm-bootstrap: done"
    dev = str(vm.device)
    lake = str(vm.lake)
    calls = vm.calls()
    # Each step in its order. Both superblock reads find no ext4 magic before mkfs, the
    # UUID is read only once mkfs and udev have run, and the chown follows the proof.
    steps = [
        f"chown root:root {lake}",
        f"chmod 0755 {lake}",
        f"chattr +i {lake}",
        f"blkid -p -o export {dev}",
        f"dd if={dev} bs=4096 skip=0 count=1 status=none",
        f"dd if={dev} bs=4096 skip=32768 count=1 status=none",
        f"mkfs.ext4 -q -m 0 -b 4096 -L marketlake {dev}",
        "udevadm settle",
        f"blkid -p -s UUID -o value {dev}",
        "sync",
        "systemctl daemon-reload",
        f"systemd-escape -p --suffix=mount {LAKE_ROOT}",
        f"systemctl start {UNIT}",
        f"findmnt -n -o UUID --mountpoint {LAKE_ROOT}",
        f"findmnt --fstab --tab-file {vm.fstab} -n -o SOURCE --mountpoint {LAKE_ROOT}",
        f"chown {OWNER}: {lake}",
        f"resize2fs {dev}",
        f"linux-install --owner {OWNER} --lake-mount {LAKE_ROOT}",
        "flock -w 600 9",
        "venv-python -m lake.vm_config render",
        f"venv-python -m lake.token_store pull --token {vm.home}/.config/marketlake/token.json",
        "venv-python -m lake.roster apply",
    ]
    assert _in_order(calls, steps), calls
    assert vm.immutable() == [lake]
    assert vm.fstab.read_text() == ROOT_LINE + _lake_line(NEW_UUID)
    assert vm.fstab.stat().st_mode & 0o777 == 0o644
    assert sorted(path.name for path in vm.fstab.parent.iterdir()) == ["fstab", "marketlake"]
    assert (vm.state / "mounted").read_text() == NEW_UUID
    # Every Python step runs as the owner, and each one reading stdin got its own file.
    venv = str(vm.checkout / ".venv" / "bin" / "python")
    assert [line for line in vm.ran("sudo ") if venv in line] == [
        f"sudo -u {OWNER} -H {venv} -m lake.vm_config render",
        f"sudo -u {OWNER} -H {venv} -m lake.token_store pull"
        f" --token {vm.home}/.config/marketlake/token.json",
        f"sudo -u {OWNER} -H {venv} -m lake.roster apply",
    ]
    assert (vm.state / "stdin-render").read_text() == VM_YAML
    tickers = (REPO_ROOT / "config" / "tickers.yaml").read_text()
    assert (vm.state / "stdin-roster").read_text() == tickers
    assert not vm.ran("mount ")
    assert not vm.ran("curl")


def test_a_second_run_changes_nothing(vm):
    first = vm.bootstrap()
    assert first.returncode == 0, first.stdout + first.stderr
    fstab = vm.fstab.read_bytes()
    stamp = vm.fstab.stat().st_mtime_ns
    second = vm.bootstrap()
    assert second.returncode == 0, second.stdout + second.stderr
    calls = vm.calls()
    for tool in ("mkfs.ext4", "dd ", "chattr", "lsattr", "chmod", "mount ", "curl", "udevadm"):
        assert not vm.ran(tool), (tool, calls)
    assert "chown root:root" not in "\n".join(calls)
    assert not [line for line in calls if line.startswith("findmnt --verify")], calls
    assert "systemctl daemon-reload" not in calls
    assert vm.fstab.read_bytes() == fstab
    assert vm.fstab.stat().st_mtime_ns == stamp
    assert vm.immutable() == [str(vm.lake)]
    # The UUID is read again on the rerun, and the mount unit's start still runs.
    assert f"blkid -p -s UUID -o value {vm.device}" in calls
    assert f"systemctl start {UNIT}" in calls
    assert (vm.state / "mounted").read_text() == NEW_UUID


# -- the format guard ------------------------------------------------------------------


def test_an_existing_ext4_mounts_without_mkfs(vm):
    vm.ext4()
    proc = vm.bootstrap()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert not vm.ran("mkfs.ext4")
    assert not vm.ran("dd ")
    assert vm.fstab.read_text() == ROOT_LINE + _lake_line(FAKE_UUID)
    assert (vm.state / "mounted").read_text() == FAKE_UUID


@pytest.mark.parametrize(
    ("probe", "magic", "env", "message"),
    [
        (None, "0", {}, "4 KiB block 0 holds an ext4 superblock"),
        (None, "32768", {}, "4 KiB block 32768 holds an ext4 superblock"),
        (None, None, {"DD_FAIL_BLOCK": "0"}, "could not read 4 KiB block 0 of"),
        (None, None, {"DD_FAIL_BLOCK": "32768"}, "could not read 4 KiB block 32768 of"),
        (None, None, {"DD_SHORT_BLOCK": "0"}, "the read of 4 KiB block 0 of"),
        (None, None, {"DD_SHORT_BLOCK": "32768"}, "the read of 4 KiB block 32768 of"),
        ("UUID=abc\nTYPE=xfs\n", None, {}, "holds something other than ext4 (blkid exit 0)"),
        ("PTUUID=abc\nPTTYPE=gpt\n", None, {}, "holds something other than ext4 (blkid exit 0)"),
        (None, None, {"BLKID_RC": "8"}, "holds something other than ext4 (blkid exit 8)"),
    ],
    ids=[
        "the primary's magic",
        "the backup's magic",
        "a read error at block 0",
        "a read error at block 32768",
        "a short read at block 0",
        "a short read at block 32768",
        "another filesystem",
        "a partition table",
        "a probe error",
    ],
)
def test_a_volume_that_may_hold_a_lake_is_never_formatted(vm, probe, magic, env, message):
    if probe is not None:
        vm.disk.joinpath("probe").write_text(probe)
    if magic is not None:
        vm.disk.joinpath(f"magic-{magic}").write_bytes(b"\x53\xef")
    proc = vm.bootstrap(**env)
    assert proc.returncode == 2, proc.stdout + proc.stderr
    line = _one_line(proc, "vm-bootstrap: ")
    assert message in line
    if magic is not None:
        assert (
            "a damaged lake, an interrupted mkfs, or a chance match on a fresh volume."
            " Nothing was formatted." in line
        )
        assert line.endswith(f"Check it read-only with: e2fsck -n {vm.device}")
    assert not vm.ran("mkfs.ext4")
    assert not vm.ran("systemctl start")
    assert vm.fstab.read_text() == ROOT_LINE
    _assert_nothing_installed(vm)


def test_the_uuid_is_read_only_after_mkfs_on_a_fresh_volume(vm):
    proc = vm.bootstrap()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    reads = [i for i, line in enumerate(vm.calls()) if line.startswith("blkid -p -s UUID")]
    assert reads and min(reads) > vm.index("udevadm settle")


@pytest.mark.parametrize(
    ("env", "line"),
    [
        ({"MKFS_RC": "1"}, "mkfs.ext4 failed"),
        ({"UDEVADM_RC": "1"}, "udevadm settle failed after mkfs.ext4"),
    ],
    ids=["mkfs", "udevadm"],
)
def test_a_failed_format_stops_before_the_fstab(vm, env, line):
    proc = vm.bootstrap(**env)
    assert proc.returncode == 1
    assert line in _one_line(proc, "vm-bootstrap: ")
    assert vm.fstab.read_text() == ROOT_LINE
    _assert_nothing_installed(vm)


# -- the mount-point directory ---------------------------------------------------------


def test_a_mounted_lake_root_is_never_touched(vm):
    vm.ext4()
    vm.mount()
    proc = vm.bootstrap()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert not vm.ran("lsattr")
    assert not vm.ran("chattr")
    assert not vm.ran(f"chmod 0755 {vm.lake}")
    assert not vm.ran("chown root")
    assert not vm.ran("blkid -p -o export")
    assert not vm.ran("mkfs.ext4")
    assert vm.immutable() == []
    # The only chown is the owner's, after the proof.
    assert vm.ran("chown") == [f"chown {OWNER}: {vm.lake}"]


def test_an_immutable_directory_is_not_prepared_again(vm):
    vm.ext4()
    vm.lake.mkdir(parents=True)
    (vm.state / "immutable").write_text(f"{vm.lake}\n")
    proc = vm.bootstrap()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert vm.ran("lsattr") == [f"lsattr -d {vm.lake}"]
    assert not vm.ran("chattr")
    assert not vm.ran(f"chmod 0755 {vm.lake}")
    assert not vm.ran("chown root")
    assert vm.immutable() == [str(vm.lake)]


def test_an_existing_directory_without_the_flag_is_made_immutable(vm):
    vm.ext4()
    vm.lake.mkdir(parents=True)
    proc = vm.bootstrap()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert vm.ran("lsattr") == [f"lsattr -d {vm.lake}"]
    assert vm.ran("chattr") == [f"chattr +i {vm.lake}"]
    assert vm.index(f"chown root:root {vm.lake}") < vm.index(f"chattr +i {vm.lake}")


# -- the UUID proof --------------------------------------------------------------------


def test_another_filesystem_mounted_there_stops_before_the_install(vm):
    vm.ext4()
    vm.mount(OTHER_UUID)
    proc = vm.bootstrap()
    assert proc.returncode == 2
    assert "is not the lake volume" in _one_line(proc, "vm-bootstrap: ")
    assert f"blkid -p -s UUID -o value {vm.device}" in vm.calls()
    assert not vm.ran("resize2fs")
    _assert_nothing_installed(vm)


def test_an_empty_mounted_uuid_never_matches(vm):
    vm.ext4()
    vm.mount("")
    proc = vm.bootstrap()
    assert proc.returncode == 2
    assert "is not the lake volume" in _one_line(proc, "vm-bootstrap: ")
    # Something with no UUID is still something mounted, so the directory is left alone.
    assert not vm.ran("lsattr")
    assert not vm.ran("chattr")
    _assert_nothing_installed(vm)


def test_a_volume_with_no_uuid_stops_before_the_fstab(vm):
    vm.ext4("")
    vm.mount("")
    proc = vm.bootstrap()
    assert proc.returncode == 2
    assert "has no filesystem UUID" in _one_line(proc, "vm-bootstrap: ")
    assert vm.fstab.read_text() == ROOT_LINE
    _assert_nothing_installed(vm)


def test_an_fstab_naming_another_source_stops_before_the_install(vm):
    proc = vm.bootstrap(FSTAB_SOURCE=f"UUID={OTHER_UUID}")
    assert proc.returncode == 2
    assert "does not mount the lake volume" in _one_line(proc, "vm-bootstrap: ")
    assert not vm.ran("resize2fs")
    _assert_nothing_installed(vm)


# -- the fstab line --------------------------------------------------------------------


def test_a_refused_fstab_is_never_written(vm):
    proc = vm.bootstrap(FINDMNT_VERIFY_RC="1")
    assert proc.returncode == 1
    assert "findmnt --verify refused" in _one_line(proc, "vm-bootstrap: ")
    assert vm.fstab.read_text() == ROOT_LINE
    assert sorted(path.name for path in vm.fstab.parent.iterdir()) == ["fstab", "marketlake"]
    assert "systemctl daemon-reload" not in vm.calls()
    assert not vm.ran("systemctl start")
    _assert_nothing_installed(vm)


def test_the_line_is_keyed_on_the_mount_point(vm):
    vm.ext4()
    vm.fstab.write_text(
        ROOT_LINE
        + f"# UUID=old {LAKE_ROOT} was the first volume\n"
        # With no space after the #, the second field is the mount point, so only the
        # comment rule keeps this line.
        + f"#UUID=old {LAKE_ROOT} ext4 defaults 0 0\n"
        + f"UUID={OTHER_UUID} {LAKE_ROOT} ext4 defaults 0 2\n"
        + "/swap.img none swap sw 0 0\n"
        + f"UUID={OTHER_UUID} {LAKE_ROOT}/sub ext4 defaults 0 2\n"
    )
    proc = vm.bootstrap()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert vm.fstab.read_text() == (
        ROOT_LINE
        + f"# UUID=old {LAKE_ROOT} was the first volume\n"
        + f"#UUID=old {LAKE_ROOT} ext4 defaults 0 0\n"
        + "/swap.img none swap sw 0 0\n"
        + f"UUID={OTHER_UUID} {LAKE_ROOT}/sub ext4 defaults 0 2\n"
        + _lake_line(FAKE_UUID)
    )
    assert "systemctl daemon-reload" in vm.calls()


def test_an_unreadable_fstab_stops_and_is_never_rewritten(vm):
    proc = vm.bootstrap(CAT_FAIL=str(vm.fstab))
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert f"could not read {vm.fstab}" in _one_line(proc, "vm-bootstrap: ")
    assert vm.fstab.read_text() == ROOT_LINE
    assert sorted(path.name for path in vm.fstab.parent.iterdir()) == ["fstab", "marketlake"]
    assert not [line for line in vm.calls() if line.startswith("findmnt --verify")]
    assert not vm.ran("systemctl")
    _assert_nothing_installed(vm)


@pytest.mark.parametrize(
    "fstab",
    # With no space after the #, the commented line's second field is /, so only the
    # comment rule keeps it from counting.
    [None, "", "/swap.img none swap sw 0 0\n", "#LABEL=cloudimg-rootfs / ext4 defaults 0 1\n"],
    ids=["missing", "empty", "no root line", "a commented root line"],
)
def test_an_fstab_with_no_root_entry_is_refused(vm, fstab):
    if fstab is None:
        vm.fstab.unlink()
    else:
        vm.fstab.write_text(fstab)
    proc = vm.bootstrap()
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert "has no entry for /" in _one_line(proc, "vm-bootstrap: ")
    if fstab is None:
        assert not vm.fstab.exists()
    else:
        assert vm.fstab.read_text() == fstab
    assert not [line for line in vm.calls() if line.startswith("findmnt --verify")]
    assert not vm.ran("systemctl")
    _assert_nothing_installed(vm)


def test_an_identical_fstab_is_not_rewritten(vm):
    vm.ext4()
    vm.fstab.write_text(ROOT_LINE + _lake_line(FAKE_UUID))
    stamp = vm.fstab.stat().st_mtime_ns
    proc = vm.bootstrap()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert vm.fstab.stat().st_mtime_ns == stamp
    assert not [line for line in vm.calls() if line.startswith("findmnt --verify")]
    assert "systemctl daemon-reload" not in vm.calls()
    assert not vm.ran("sync")


# -- the mount and the resize ----------------------------------------------------------


def test_a_failed_mount_stops_before_the_install(vm):
    proc = vm.bootstrap(FAIL_START=UNIT)
    assert proc.returncode == 1
    assert f"systemctl start {UNIT} failed" in _one_line(proc, "vm-bootstrap: ")
    assert not vm.ran("resize2fs")
    _assert_nothing_installed(vm)


def test_a_failed_resize_stops_before_the_install(vm):
    proc = vm.bootstrap(RESIZE2FS_RC="1")
    assert proc.returncode == 1
    assert "resize2fs failed" in _one_line(proc, "vm-bootstrap: ")
    assert vm.ran("resize2fs") == [f"resize2fs {vm.device}"]
    assert not vm.ran("linux-install")


def test_a_volume_that_never_appears_stops_before_the_install(vm):
    vm.device.unlink()
    proc = vm.bootstrap()
    assert proc.returncode == 1
    assert "did not appear" in _one_line(proc, "vm-bootstrap: ")
    assert vm.ran("sleep") == ["sleep 5"] * 60
    assert not vm.ran("findmnt")
    assert not vm.ran("systemctl")
    _assert_nothing_installed(vm)


# -- uv --------------------------------------------------------------------------------


@pytest.mark.parametrize("installed", [None, "0.10.0"], ids=["absent", "another version"])
def test_uv_is_installed_when_the_version_differs(vm, installed):
    vm.set_uv(installed)
    proc = vm.bootstrap()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    [curl] = vm.ran("curl")
    installer = curl.split(" -o ")[1].split()[0]
    assert curl == (
        f"curl --proto =https --tlsv1.2 -fsSL --retry 5 --retry-all-errors -o {installer}"
        f" https://astral.sh/uv/{UV_VERSION}/install.sh"
    )
    # mktemp makes the file in the test's temporary directory, so the check below that it
    # is empty afterwards checks something.
    assert f"mktemp {installer}" in vm.calls()
    assert installer.startswith(f"{vm.temp}/")
    # mktemp makes the file 0600 and root's, so the owner reads it only after the chmod.
    run = f"sudo -u {OWNER} -H env UV_NO_MODIFY_PATH=1 sh {installer}"
    assert vm.index(f"chmod 0644 {installer}") < vm.index(run)
    assert "uv-installer mode -rw-r--r--" in vm.calls()
    assert "uv-installer UV_NO_MODIFY_PATH=1" in vm.calls()
    assert vm.index("uv-installer UV_NO_MODIFY_PATH=1") < vm.index(
        f"linux-install --owner {OWNER} --lake-mount {LAKE_ROOT}"
    )
    assert not Path(installer).exists()
    assert list(vm.temp.iterdir()) == []


def test_a_failed_uv_download_stops_before_the_install(vm):
    vm.set_uv(None)
    proc = vm.bootstrap(CURL_RC="22")
    assert proc.returncode == 1
    assert "could not download" in _one_line(proc, "vm-bootstrap: ")
    assert [line for line in vm.ran("mktemp ") if line.startswith(f"mktemp {vm.temp}/")]
    assert list(vm.temp.iterdir()) == []
    _assert_nothing_installed(vm, chowned=True)


def test_a_failed_uv_installer_stops_and_leaves_no_file(vm):
    vm.set_uv(None)
    proc = vm.bootstrap(INSTALLER_RC="1")
    assert proc.returncode == 1
    assert f"the uv {UV_VERSION} installer failed" in _one_line(proc, "vm-bootstrap: ")
    [curl] = vm.ran("curl")
    installer = curl.split(" -o ")[1].split()[0]
    assert installer.startswith(f"{vm.temp}/")
    assert "uv-installer UV_NO_MODIFY_PATH=1" in vm.calls()
    assert not Path(installer).exists()
    assert list(vm.temp.iterdir()) == []
    _assert_nothing_installed(vm, chowned=True)


@pytest.mark.parametrize(
    "pin",
    # The second's version starts with a plain x.y.z, so only an anchored end refuses it.
    ["python 3.12.4\n", f"uv {UV_VERSION}/../x\n"],
    ids=["no uv line", "a version with a tail"],
)
def test_a_tool_versions_without_a_plain_uv_pin_is_refused(vm, pin):
    (vm.checkout / ".tool-versions").write_text(pin)
    proc = vm.bootstrap()
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert "must pin uv as a line 'uv <x.y.z>'" in _one_line(proc, "vm-bootstrap: ")
    assert not vm.ran("curl")
    assert not vm.ran("linux-install")


# -- the install, the lock and the retries ---------------------------------------------


def test_the_lock_is_taken_only_after_the_install_returns(vm):
    proc = vm.bootstrap()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    install = vm.index(f"linux-install --owner {OWNER} --lake-mount {LAKE_ROOT}")
    lock = vm.index("flock -w 600 9")
    render = vm.index("venv-python -m lake.vm_config render")
    assert install < lock < render
    # One lock for each of the render, the pull and the roster.
    assert vm.ran("flock") == ["flock -w 600 9"] * 3
    assert vm.lock.exists()


def test_each_attempt_takes_the_lock_and_the_wait_runs_without_it(vm):
    proc = vm.bootstrap(RENDER_RCS="3 1 0", PULL_RCS="1 0")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    # The fake sleep marks a wait that inherits the lock's open descriptor, so a lock
    # held across the retries shows in its line.
    steps = [line for line in vm.calls() if line.startswith(("flock", "sleep", "venv-python -m"))]
    render = "venv-python -m lake.vm_config render"
    pull = f"venv-python -m lake.token_store pull --token {vm.home}/.config/marketlake/token.json"
    lock = "flock -w 600 9"
    assert steps == [
        lock, render, "sleep 20",
        lock, render, "sleep 20",
        lock, render,
        lock, pull, "sleep 20",
        lock, pull,
        lock, "venv-python -m lake.roster apply",
    ]  # fmt: skip


def test_a_transient_install_failure_is_retried(vm):
    proc = vm.bootstrap(INSTALL_RCS="1 1 0")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert len(vm.ran("linux-install")) == 3
    assert vm.ran("sleep") == ["sleep 30"] * 2


@pytest.mark.parametrize(
    ("rcs", "runs", "line"),
    [
        ("2", 1, "exited 2, a refusal"),
        ("1", 5, "exited 1 on all 5 attempts"),
    ],
    ids=["refusal", "every attempt"],
)
def test_an_install_that_keeps_failing_stops_the_bootstrap(vm, rcs, runs, line):
    proc = vm.bootstrap(INSTALL_RCS=rcs)
    assert proc.returncode == 1
    assert line in proc.stderr
    assert len(vm.ran("linux-install")) == runs
    assert not vm.ran("flock")
    assert not vm.ran("venv-python")


@pytest.mark.parametrize("rcs", ["3 1 0", "1 3 3 0"])
def test_the_render_retries_exits_1_and_3(vm, rcs):
    proc = vm.bootstrap(RENDER_RCS=rcs)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    tries = len(rcs.split())
    assert len(vm.ran("venv-python -m lake.vm_config")) == tries
    assert vm.ran("sleep") == ["sleep 20"] * (tries - 1)
    assert len(vm.ran("venv-python -m lake.token_store")) == 1
    assert len(vm.ran("venv-python -m lake.roster")) == 1


def test_a_render_refusal_is_final_and_skips_what_reads_config(vm):
    proc = vm.bootstrap(RENDER_RCS="2")
    assert proc.returncode == 1
    assert len(vm.ran("venv-python -m lake.vm_config")) == 1
    assert not vm.ran("sleep")
    assert not vm.ran("venv-python -m lake.token_store")
    assert not vm.ran("venv-python -m lake.roster")
    assert "the config render exited 2, which is not retried" in proc.stderr
    skipped = "the token pull and the roster are skipped, because both read config.yaml"
    assert f"vm-bootstrap: {skipped}" in proc.stderr.splitlines()


def test_the_render_retries_are_bounded(vm):
    proc = vm.bootstrap(RENDER_RCS="3")
    assert proc.returncode == 1
    assert len(vm.ran("venv-python -m lake.vm_config")) == 6
    assert vm.ran("sleep") == ["sleep 20"] * 5
    assert "the config render exited 3 on all 6 attempts" in proc.stderr
    assert not vm.ran("venv-python -m lake.token_store")


def test_the_pull_retries_exit_3_and_stops_on_2(vm):
    retried = vm.bootstrap(PULL_RCS="3 0")
    assert retried.returncode == 0, retried.stdout + retried.stderr
    assert len(vm.ran("venv-python -m lake.token_store")) == 2
    (vm.state / "count-pull").unlink()
    refused = vm.bootstrap(PULL_RCS="2")
    assert refused.returncode == 1
    assert len(vm.ran("venv-python -m lake.token_store")) == 1
    assert "the token pull exited 2, which is not retried" in refused.stderr
    # The roster does not need the token, so it still runs.
    assert len(vm.ran("venv-python -m lake.roster")) == 1


def test_a_roster_refusal_prints_and_finishes_the_run(vm):
    proc = vm.bootstrap(ROSTER_RCS="2")
    assert proc.returncode == 1
    assert len(vm.ran("venv-python -m lake.roster")) == 1
    assert "vm-bootstrap: the roster apply exited 2" in proc.stderr.splitlines()
    assert proc.stderr.splitlines()[-1] == "vm-bootstrap: finished with a failed step, listed above"


def test_a_lock_that_never_frees_skips_the_python_steps(vm):
    proc = vm.bootstrap(FLOCK_RC="1")
    assert proc.returncode == 1
    assert vm.ran("linux-install")
    assert not vm.ran("venv-python")
    assert "held" in proc.stderr


# -- refusals before anything runs -----------------------------------------------------


def _conf(text: str):
    return lambda vm: vm.conf.write_text(text)


def _vm_yaml(text: str):
    return lambda vm: (vm.checkout / "config" / "vm.yaml").write_text(text)


_PRODUCTION_NOT_ROOT = {cp.INSTALL_ROOT_ENV: "", cp.INSTALL_TEST_ENV: "", "FAKE_UID": "1000"}
_VOLUME_TWICE = f"OWNER={OWNER}\nLAKE_VOLUME_ID={VOLUME_ID}\nLAKE_VOLUME_ID={VOLUME_ID}\n"
_NOT_A_NAME = "must set OWNER to an account name"

PREFLIGHT = [
    ("not root", lambda vm: None, {"FAKE_UID": "1000"}, "run this as root"),
    # A production run sets neither variable, so the test-root guard must let it reach the
    # root check, where it stops.
    ("production, not root", lambda vm: None, _PRODUCTION_NOT_ROOT, "run this as root"),
    ("leaked root", lambda vm: None, {cp.INSTALL_TEST_ENV: ""}, "only a test may set"),
    ("no conf", lambda vm: vm.conf.unlink(), {}, "is missing"),
    ("unknown key", _conf(f"OWNER={OWNER}\nLAKE_VOLUME_ID={VOLUME_ID}\nX=1\n"), {}, "unknown key"),
    ("no volume", _conf(f"OWNER={OWNER}\n"), {}, "LAKE_VOLUME_ID to a volume id"),
    ("bad volume", _conf(f"OWNER={OWNER}\nLAKE_VOLUME_ID=vol-XYZ\n"), {}, "volume id"),
    ("volume with a tail", _conf(f"OWNER={OWNER}\nLAKE_VOLUME_ID=vol-0abcXYZ\n"), {}, "volume id"),
    ("volume twice", _conf(_VOLUME_TWICE), {}, "sets LAKE_VOLUME_ID twice"),
    ("twice", _conf(f"OWNER={OWNER}\nOWNER={OWNER}\nLAKE_VOLUME_ID={VOLUME_ID}\n"), {}, "twice"),
    ("bad owner", _conf(f"OWNER=a b\nLAKE_VOLUME_ID={VOLUME_ID}\n"), {}, "account name"),
    ("no account", _conf(f"OWNER=nobody-here\nLAKE_VOLUME_ID={VOLUME_ID}\n"), {}, "no account"),
    # getent resolves 1000 too, so only the name rule prints this exact line.
    ("a uid", _conf(f"OWNER=1000\nLAKE_VOLUME_ID={VOLUME_ID}\n"), {}, _NOT_A_NAME),
    ("no home", lambda vm: None, {"FAKE_GETENT_HOME": ""}, "has no home directory"),
    ("not key=value", _conf("$(touch /tmp/x)\n"), {}, "not KEY=VALUE"),
    ("relative root", _vm_yaml("lake_root: srv/marketlake\n"), {}, "absolute path"),
    ("spaced root", _vm_yaml("lake_root: /srv/market lake\n"), {}, "absolute path"),
    ("dotted root", _vm_yaml("lake_root: /srv/../etc\n"), {}, ". or .."),
    ("a dot component", _vm_yaml("lake_root: /srv/./marketlake\n"), {}, ". or .."),
    ("no root key", _vm_yaml("role: shadow\n"), {}, "could not read a lake_root"),
    ("root not a string", _vm_yaml("lake_root: [1]\n"), {}, "could not read a lake_root"),
]


@pytest.mark.parametrize("script", [BOOTSTRAP, EMPTY], ids=lambda path: path.name)
@pytest.mark.parametrize(
    ("setup", "env", "message"),
    [case[1:] for case in PREFLIGHT],
    ids=[case[0] for case in PREFLIGHT],
)
def test_a_bad_host_is_refused_before_any_disk_step(vm, script, setup, env, message):
    vm.ext4()
    vm.mount()
    setup(vm)
    proc = vm.run([str(vm.checkout / "deploy" / script.name)], **env)
    assert proc.returncode == 2, proc.stdout + proc.stderr
    prefix = f"{script.stem}: "
    assert message in _one_line(proc, prefix)
    calls = vm.calls()
    for tool in ("blkid", "findmnt", "mkfs", "chattr", "chown", "systemctl", "flock", "rm "):
        assert not vm.ran(tool), (tool, calls)
    assert vm.fstab.read_text() == ROOT_LINE


@pytest.mark.parametrize("script", [BOOTSTRAP, EMPTY], ids=lambda path: path.name)
def test_an_argument_is_refused_before_anything_runs(vm, script):
    vm.ext4()
    vm.mount()
    proc = vm.run([str(vm.checkout / "deploy" / script.name), "--help"])
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert "takes no arguments" in _one_line(proc, f"{script.stem}: ")
    assert vm.calls() == []
    assert vm.fstab.read_text() == ROOT_LINE


# -- vm-empty-shadow-lake.sh -----------------------------------------------------------


def _shadow(vm: VM, role: str | None = "role: shadow\n") -> None:
    """A shadow VM whose units are stopped, with a lake that holds files."""
    vm.ext4()
    vm.mount()
    vm.fstab.write_text(ROOT_LINE + _lake_line(FAKE_UUID))
    for name in ("manifest.jsonl", "segments/QQQ/one.parquet", "journal/outbox/a.jsonl"):
        path = vm.lake / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x")
    (vm.lake / "lost+found").mkdir()
    (vm.lake / "lost+found" / "#12").write_text("kept")
    (vm.lake / ".hidden").write_text("x")
    if role is not None:
        config = vm.home / ".config" / "marketlake" / "config.yaml"
        config.parent.mkdir(parents=True, exist_ok=True)
        config.write_text(role)
    units = vm.root / cp.SYSTEMD_UNIT_DIR.lstrip("/")
    units.mkdir(parents=True)
    for unit in ("com.marketlake.daemon.service", "com.marketlake.sunday.timer"):
        (units / unit).write_text("")


def _lake_entries(vm: VM) -> list[str]:
    return sorted(str(path.relative_to(vm.lake)) for path in vm.lake.rglob("*"))


def test_the_empty_script_keeps_only_lost_and_found(vm):
    _shadow(vm)
    (vm.state / "failed").mkdir()
    (vm.state / "failed" / "com.marketlake.daemon.service").write_text("")
    proc = vm.empty()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _lake_entries(vm) == ["lost+found", "lost+found/#12"]
    calls = vm.calls()
    assert vm.ran("flock") == ["flock -n 9"]
    [rm] = vm.ran("rm ")
    assert rm.startswith("rm -rf --one-file-system -- ")
    assert vm.index("flock -n 9") < calls.index(rm)
    assert f"blkid -p -s UUID -o value {vm.device}" in calls
    assert "systemctl list-units --all --no-legend --plain com.marketlake.*" in calls
    assert calls.index(f"findmnt -n -R -o TARGET {LAKE_ROOT}") < calls.index(rm)


def _no_uuid_anywhere(vm: VM) -> None:
    """A device, a mount and an fstab line that all read an empty UUID, so they agree."""
    vm.ext4("")
    vm.mount("")
    vm.fstab.write_text(ROOT_LINE + _lake_line(""))


def _start(vm: VM, unit: str) -> None:
    (vm.state / "pid").mkdir(exist_ok=True)
    (vm.state / "pid" / unit).write_text("1001")


EMPTY_REFUSALS = [
    ("not mounted", lambda vm: (vm.state / "mounted").unlink(), {}, "is not the filesystem"),
    ("another volume", lambda vm: vm.mount(OTHER_UUID), {}, "is not the filesystem"),
    ("every uuid empty", lambda vm: _no_uuid_anywhere(vm), {}, "is not the filesystem"),
    ("no fstab line", lambda vm: vm.fstab.write_text(ROOT_LINE), {}, "is not the filesystem"),
    ("not attached", lambda vm: vm.device.unlink(), {}, "is not attached"),
    ("no config", lambda vm: _role(vm, None), {}, "is missing"),
    ("unparsable", lambda vm: _role(vm, "role: [shadow\n"), {}, "does not parse"),
    ("no role", lambda vm: _role(vm, "lake_root: /srv/marketlake\n"), {}, "sets no role"),
    ("primary", lambda vm: _role(vm, "role: primary\n"), {}, "does not set role to shadow"),
    ("capitalised", lambda vm: _role(vm, "role: Shadow\n"), {}, "does not set role to shadow"),
    ("a list", lambda vm: _role(vm, "role: [shadow]\n"), {}, "does not set role to shadow"),
    ("a daemon", lambda vm: _start(vm, "com.marketlake.daemon.service"), {}, "not stopped"),
    ("a timer", lambda vm: _start(vm, "com.marketlake.sunday.timer"), {}, "not stopped"),
    ("lock held", lambda vm: None, {"FLOCK_RC": "1"}, "another run holds"),
    ("list fails", lambda vm: None, {"LIST_UNITS_RC": "1"}, "list-units failed"),
    ("a submount", lambda vm: _submount(vm), {}, "is mounted below"),
    ("tree fails", lambda vm: None, {"FINDMNT_TREE_RC": "1"}, "could not list the mounts"),
]


def _submount(vm: VM) -> None:
    """Mount something at one of the lake's top-level entries."""
    (vm.state / "submounts").write_text(f"{LAKE_ROOT}/segments\n")


def _role(vm: VM, text: str | None) -> None:
    config = vm.home / ".config" / "marketlake" / "config.yaml"
    if text is None:
        config.unlink()
    else:
        config.write_text(text)


@pytest.mark.parametrize(
    ("setup", "env", "message"),
    [case[1:] for case in EMPTY_REFUSALS],
    ids=[case[0] for case in EMPTY_REFUSALS],
)
def test_the_empty_script_refuses_and_deletes_nothing(vm, setup, env, message):
    _shadow(vm)
    before = _lake_entries(vm)
    setup(vm)
    proc = vm.empty(**env)
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert message in _one_line(proc, "vm-empty-shadow-lake: ")
    assert _lake_entries(vm) == before
    assert not vm.ran("rm ")


def test_a_waiting_timer_names_itself_in_the_refusal(vm):
    _shadow(vm)
    _start(vm, "com.marketlake.sunday.timer")
    proc = vm.empty()
    line = _one_line(proc, "vm-empty-shadow-lake: ")
    assert "com.marketlake.sunday.timer" in line
    assert "com.marketlake.daemon.service" not in line


def test_no_loaded_units_lets_the_delete_proceed(vm):
    """With no unit loaded, list-units prints nothing, which is no running unit."""
    _shadow(vm)
    units = vm.root / cp.SYSTEMD_UNIT_DIR.lstrip("/")
    for unit in units.iterdir():
        unit.unlink()
    proc = vm.empty()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _lake_entries(vm) == ["lost+found", "lost+found/#12"]


@pytest.mark.parametrize("script", [BOOTSTRAP, EMPTY], ids=lambda path: path.name)
@pytest.mark.parametrize(
    "conf",
    [
        f"OWNER={OWNER}\nLAKE_VOLUME_ID={VOLUME_ID}",
        f"OWNER={OWNER}\n\nLAKE_VOLUME_ID={VOLUME_ID}\n",
    ],
    ids=["no final newline", "a blank line"],
)
def test_a_conf_the_parser_accepts_runs(vm, script, conf):
    _shadow(vm)
    vm.conf.write_text(conf)
    proc = vm.run([str(vm.checkout / "deploy" / script.name)])
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert proc.stdout.splitlines()[-1].startswith(f"{script.stem}: done")
