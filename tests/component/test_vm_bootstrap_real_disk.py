"""The bootstrap's format guard, run by the real disk tools against ext4 images.

The fake disk in ``tests.support.fake_disk`` answers whatever a test tells it to. Only the
real ``blkid``, ``dd``, ``od`` and ``mkfs.ext4`` can say what a real volume reads as. They
exist on CI's Linux runner and not on a Mac, so these tests skip where they are absent and
fail where they are absent under ``CI``, through ``tests.support.real_tools``.

The volume is a sparse 256 MiB image at its by-id path. Every tool but those four is the
usual fake, so a run that passes the guard stops later, at the fake mount, which reads
the fake disk the real ``mkfs.ext4`` never wrote. These tests check only what the guard
decided. A 256 MiB image gets 4 KiB blocks only because the bootstrap passes ``-b 4096``,
since ``mke2fs.conf``'s "small" type would choose 1 KiB blocks, which would put no backup
superblock at 4 KiB block 32768.
"""

from __future__ import annotations

import random
import subprocess
from pathlib import Path

import pytest

from tests.component.test_vm_bootstrap import VM, build_tools
from tests.support.real_tools import require_tool

SIZE = 256 * 2**20
BLOCK = 4096
FORMATS = "the lake volume holds no ext4 superblock, so it gets an ext4 filesystem"
MOUNTS = "the lake volume holds an ext4 filesystem, so it mounts without formatting"


@pytest.fixture(scope="module")
def real_disk(tmp_path_factory) -> tuple[Path, str]:
    """The harness tools with the real disk tools over the fakes, and the real blkid."""
    found = {name: require_tool(name) for name in ("blkid", "mkfs.ext4", "dd", "od")}
    shared = build_tools(tmp_path_factory.mktemp("vm-real-disk"))
    # Each real tool replaces the fake's link. The fake's body stays under bin/.fake,
    # where nothing reads it once the name points at the real tool.
    for name, path in found.items():
        link = shared / "bin" / name
        link.unlink(missing_ok=True)
        link.symlink_to(path)
    return shared, found["blkid"]


@pytest.fixture
def vm(tmp_path, real_disk) -> VM:
    vm = VM(tmp_path, real_disk[0])
    with vm.device.open("r+b") as image:
        image.truncate(SIZE)
    return vm


def _probe(blkid: str, vm: VM) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [blkid, "-p", "-o", "export", str(vm.device)], capture_output=True, text=True
    )


def _write(vm: VM, offset: int, data: bytes) -> None:
    with vm.device.open("r+b") as image:
        image.seek(offset)
        image.write(data)


def _format(vm: VM) -> None:
    """Format the image the way the bootstrap does, by running the bootstrap on it."""
    proc = vm.bootstrap()
    assert FORMATS in proc.stdout, proc.stdout + proc.stderr


def test_a_fresh_volume_of_random_content_is_formatted(vm, real_disk):
    # The first boot found block 0 zero and high-entropy content in every block past it.
    rng = random.Random(686)
    for block in [*range(1, 256), *range(32760, 32776)]:
        _write(vm, block * BLOCK, rng.randbytes(BLOCK))
    with vm.device.open("rb") as image:
        image.seek(32768 * BLOCK + 56)
        assert image.read(2) != b"\x53\xef"
    proc = vm.bootstrap()
    assert FORMATS in proc.stdout, proc.stdout + proc.stderr
    assert "TYPE=ext4" in _probe(real_disk[1], vm).stdout.splitlines()


def test_the_bootstraps_own_filesystem_mounts_without_formatting(vm, real_disk):
    _format(vm)
    uuid = _probe(real_disk[1], vm).stdout
    proc = vm.bootstrap()
    assert MOUNTS in proc.stdout, proc.stdout + proc.stderr
    assert FORMATS not in proc.stdout
    assert _probe(real_disk[1], vm).stdout == uuid


def test_a_lake_with_a_zeroed_primary_superblock_is_not_formatted(vm, real_disk):
    _format(vm)
    _write(vm, 0, bytes(BLOCK))
    assert _probe(real_disk[1], vm).returncode == 2
    proc = vm.bootstrap()
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert "4 KiB block 32768 holds an ext4 superblock" in proc.stderr
    with vm.device.open("rb") as image:
        assert image.read(BLOCK) == bytes(BLOCK)


def test_a_lake_whose_primary_fails_its_checksum_is_not_formatted(vm, real_disk):
    _format(vm)
    # One byte of the volume label, at 0x78 in the superblock, so the magic survives and
    # the metadata_csum checksum does not. blkid then reports no filesystem at all.
    with vm.device.open("rb") as image:
        image.seek(1024 + 0x78)
        label = image.read(1)
    _write(vm, 1024 + 0x78, bytes([label[0] ^ 0xFF]))
    assert _probe(real_disk[1], vm).returncode == 2
    proc = vm.bootstrap()
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert "4 KiB block 0 holds an ext4 superblock" in proc.stderr
