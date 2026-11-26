"""The reader-only install and the capture install split by one dependency group (#1024).

A consumer that reads the lake installs the package itself, which carries only what reading
needs. Every ``uv sync`` in a checkout also installs the ``capture`` group, which holds what
only capture, the bucket jobs and the dashboard import. These tests cross one real boundary,
the installer, so they sit in the component tier.

1. The reader install, synced offline from uv.lock into a clean virtualenv, imports every
   module a consumer imports and holds none of the capture-only packages.
2. Those modules' own source imports no capture-only package anywhere, inside functions
   included, since an import there fails only when the function runs.
3. The README's "Which install to use" lists exactly those modules.
4. The capture install, read from ``uv export`` under the flags ``deploy/linux-install.sh``
   syncs with, holds every third-party package any module in ``src/lake`` imports.
"""

from __future__ import annotations

import ast
import importlib.metadata
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE = REPO_ROOT / "src" / "lake"
LINUX_INSTALL = REPO_ROOT / "deploy" / "linux-install.sh"
README = REPO_ROOT / "README.md"

# Every lake module l3a0/ithildin-runtime's code and tests import, read at its 09c4547. Its
# code imports actions, config, loader, manifest, paths and unsealed, and its tests import
# the rest to build and check a fixture lake.
READER_MODULES = (
    "lake.actions",
    "lake.bar_import",
    "lake.battery",
    "lake.calendar",
    "lake.capture_spans",
    "lake.config",
    "lake.gap",
    "lake.journal",
    "lake.loader",
    "lake.manifest",
    "lake.paths",
    "lake.schema_versions",
    "lake.security_master",
    "lake.segment_join",
    "lake.unsealed",
)


def _normalize(name: str) -> str:
    """A distribution name in PEP 503's normal form, so ``PyYAML`` and ``pyyaml`` match."""
    return re.sub(r"[-_.]+", "-", name).lower()


def _uv() -> str:
    uv = os.environ.get("UV") or shutil.which("uv")
    if uv is None:
        pytest.fail("uv is not on PATH, and these tests run the installer the hosts run")
    return uv


def _run(args: list[str], cwd: Path, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(args, cwd=cwd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        pytest.fail(
            f"{shlex.join(args)} exited {result.returncode}. An offline sync needs the wheels "
            f"uv.lock pins, and the hatchling that builds the package, in uv's cache, where "
            f"the checkout's own `uv sync` puts them.\n{result.stderr}"
        )
    return result.stdout


def _export(flags: list[str]) -> set[str]:
    """The distributions ``uv export --frozen`` lists under ``flags``, without the project."""
    args = [_uv(), "export", "--frozen", "--no-hashes", "--no-emit-project", *flags]
    text = _run(args, REPO_ROOT)
    return {
        _normalize(line.split("==", 1)[0])
        for line in text.splitlines()
        if line and not line[0].isspace() and not line.startswith("#") and "==" in line
    }


def _linux_install_sync_flags() -> list[str]:
    """The flags the VM's install passes to ``uv sync``, read from the script it runs."""
    found = re.findall(r'/uv" sync ([^\n#]*)', LINUX_INSTALL.read_text())
    assert len(found) == 1, f"expected one uv sync in {LINUX_INSTALL.name}, found {found}"
    return [flag for flag in shlex.split(found[0]) if flag != "--frozen"]


def _capture_only() -> set[str]:
    """The distributions the capture install holds and the reader install does not."""
    capture_only = _export(_linux_install_sync_flags()) - _export(["--no-default-groups"])
    # The split as pyproject.toml states it today, so a derivation gone empty cannot pass
    # for a clean install. Moving a package across the split changes this line too.
    assert {"schwab-py", "boto3", "duckdb"} <= capture_only, capture_only
    return capture_only


def test_reader_install_imports_every_reader_module_without_capture_packages(tmp_path):
    uv = _uv()
    capture_only = _capture_only()

    # The package's own dependencies and no group, at uv.lock's versions, with the package
    # built as a wheel rather than linked to the checkout. A frozen sync installs from the
    # lock's URLs, so it works offline from what the checkout's own sync cached. A resolve,
    # as `uv pip install` does, would need index pages a frozen sync never fetched. uv keys
    # its cached build of a local package on pyproject.toml's modification time, so an edit
    # under src/lake alone would reuse a stale wheel without --reinstall-package.
    venv = tmp_path / "venv"
    _run(
        [
            uv, "sync", "--quiet", "--frozen", "--offline", "--no-default-groups",
            "--no-editable", "--reinstall-package", "marketlake",
            "--python", sys.executable, "--project", str(REPO_ROOT),
        ],
        tmp_path,
        env={**os.environ, "UV_PROJECT_ENVIRONMENT": str(venv)},
    )  # fmt: skip
    python = venv / "bin" / "python"

    # -I keeps the working directory and every PYTHON* variable off sys.path, and the
    # working directory sits outside the checkout, so lake can only come from the install.
    probe = f"""
import importlib, importlib.metadata, json, sys
files = {{name: importlib.import_module(name).__file__ for name in {READER_MODULES!r}}}
installed = sorted(d.metadata["Name"] for d in importlib.metadata.distributions())
print(json.dumps({{"files": files, "installed": installed, "prefix": sys.prefix}}))
"""
    report = json.loads(_run([str(python), "-I", "-c", probe], tmp_path))

    site = Path(report["prefix"]).resolve()
    for name, file in report["files"].items():
        assert Path(file).resolve().is_relative_to(site), f"{name} came from {file}, not {site}"
    installed = {_normalize(name) for name in report["installed"]}
    assert "marketlake" in installed
    assert not installed & capture_only, f"the reader install holds {installed & capture_only}"


def _third_party_imports(paths: list[Path] | None = None) -> dict[str, set[str]]:
    """Every top-level module name the files import outside the standard library.

    The files default to all of ``src/lake``. Function-local imports count, since
    ``lake.schwab`` imports ``schwab`` only inside its functions and capture still needs it.
    """
    found: dict[str, set[str]] = {}
    for path in paths if paths is not None else sorted(SOURCE.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                names = [node.module]
            else:
                continue
            for name in names:
                top = name.split(".", 1)[0]
                if top != "lake" and top not in sys.stdlib_module_names:
                    found.setdefault(top, set()).add(path.name)
    return found


def test_reader_modules_import_no_capture_package_anywhere():
    capture_only = _capture_only()
    providers = importlib.metadata.packages_distributions()
    files = [SOURCE / f"{name.removeprefix('lake.')}.py" for name in READER_MODULES]
    found = {
        top: sorted(names)
        for top, names in _third_party_imports(files).items()
        if {_normalize(d) for d in providers.get(top, [])} & capture_only
    }
    assert not found, f"reader modules import capture-only packages: {found}"


def test_readme_lists_the_reader_modules():
    section = README.read_text().split("\n## Which install to use\n", 1)[1].split("\n## ", 1)[0]
    listed = section.split("The reader install imports", 1)[1].split("Any other module", 1)[0]
    assert set(re.findall(r"`(lake\.[a-z_]+)`", listed)) == set(READER_MODULES)


def test_capture_install_carries_every_package_lake_imports():
    capture = _export(_linux_install_sync_flags())
    providers = importlib.metadata.packages_distributions()
    needed: dict[str, set[str]] = {}
    for top, files in _third_party_imports().items():
        assert top in providers, f"{sorted(files)} import {top}, which nothing installed provides"
        for distribution in providers[top]:
            needed.setdefault(_normalize(distribution), set()).update(files)
    # schwab is imported only inside functions, so this fails if the scan skips them.
    assert "schwab-py" in needed
    missing = {name: sorted(files) for name, files in needed.items() if name not in capture}
    assert not missing, f"the capture install leaves out packages lake imports: {missing}"
