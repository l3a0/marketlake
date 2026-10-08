"""One program behind every fake, so a Mac scans one new file per process.

macOS scans a newly written file the first time it runs as a program. Under load the scan
took 0.3 to 2.3 seconds a file, and the tests' fakes once paid it file by file. The scan
is paid per inode: a symlink to a file that already ran costs about 3 ms, while a copy
pays again even with identical bytes, and a file that bash reads rather than runs pays
nothing.

So each fake here is a symlink to one dispatcher script, written once per process, and
the fake's body is a plain file the dispatcher sources. A fake at ``P`` keeps its body at
``P.parent / ".fake" / P.name``. The dispatcher finds the body from ``$0``, the path the
fake was run by. When the link's own directory holds no body, as for a checkout's
``.venv/bin/python`` linked to a shared ``tools/python``, it follows the link one hop at a
time and looks again.

A sourced body shares the dispatcher's shell, so the dispatcher keeps that shell as bash
gives it to a script. It sets neither ``-e`` nor ``-u``, exports nothing, leaves
descriptor 9 alone, defines no functions, and sources the body as its last act, with the
arguments untouched. The one variable it leaves set is the body's own path, which the
``.`` needs and which is not exported.

Two things still differ from running the body as its own script, because the body is
sourced.

1. ``${BASH_SOURCE[0]}`` is the body's path, ``<dir>/.fake/<name>``, rather than the
   fake's. So a real script that finds its own files from ``BASH_SOURCE`` must not be
   installed through ``install``.
2. A top-level ``return`` ends the fake with its status. A script run on its own prints
   an error there and carries on.

The dispatcher finds a body beside a link before it follows the link. So a link
re-pointed with ``symlink_to``, in a directory that still holds an older
``.fake/<name>``, runs that stale body. ``install`` avoids this by writing the body beside
every link it makes.

The dispatcher's mode is read-only. So for any user but root, opening a fake's path for
writing raises rather than overwriting the program every fake shares, and ``install``
unlinks first. The mode does not stop a chmod through a link. ``Path.chmod`` follows the
link, and an owner may change the mode of a file whatever the mode is.
"""

from __future__ import annotations

import atexit
import functools
import os
import shutil
import tempfile
from pathlib import Path

BODY_DIR = ".fake"
MAX_HOPS = 8

# Written for bash 3.2 as well as a current bash, because the tests run it on a Mac.
DISPATCHER = rf"""#!/bin/bash
# Every fake is a symlink to this script, so a Mac scans this one file once per process
# rather than each fake on its first run. It sources the body named by the path it was
# run by, from the .fake directory beside that path, following a chain of links one hop
# at a time when the first directory holds none.
__fake_path="$0"
__fake_hops=0
while :; do
  case "$__fake_path" in
    */*) __fake_dir="${{__fake_path%/*}}" ;;
    *) __fake_dir=. ;;
  esac
  __fake_body="$__fake_dir/{BODY_DIR}/${{__fake_path##*/}}"
  if [[ -f "$__fake_body" ]]; then break; fi
  if [[ ! -L "$__fake_path" || $__fake_hops -ge {MAX_HOPS} ]]; then
    printf 'fake: no body for %s\n' "$0" >&2
    exit 127
  fi
  __fake_target="$(/usr/bin/readlink "$__fake_path")"
  case "$__fake_target" in
    /*) __fake_path="$__fake_target" ;;
    *) __fake_path="$__fake_dir/$__fake_target" ;;
  esac
  __fake_hops=$((__fake_hops + 1))
done
unset __fake_path __fake_hops __fake_dir __fake_target
. "$__fake_body"
"""


@functools.cache
def dispatcher() -> Path:
    """The dispatcher for this process, written on the first call and removed at exit."""
    directory = Path(tempfile.mkdtemp(prefix="fake-bin-"))
    owner = os.getpid()

    def remove() -> None:
        # A forked child inherits this hook, and must not remove its parent's dispatcher.
        if os.getpid() == owner:
            shutil.rmtree(directory, ignore_errors=True)

    atexit.register(remove)
    path = directory / "dispatch"
    path.write_text(DISPATCHER)
    path.chmod(0o555)
    return path


def body_path(path: Path) -> Path:
    """Where the body of the fake at ``path`` lives."""
    return path.parent / BODY_DIR / path.name


def checked_links(root: Path, *allowed: Path) -> dict[str, Path]:
    """Every link under ``root``, by its path relative to ``root``, resolved.

    Each link must end at the dispatcher or at one of ``allowed``, and no other file under
    ``root`` may be executable. So a fake written as its own file fails here, as does a
    link to anything else.
    """
    targets = {dispatcher().resolve(), *(path.resolve() for path in allowed)}
    found = {}
    for directory, dirnames, filenames in os.walk(root):
        for name in dirnames + filenames:
            path = Path(directory) / name
            relative = str(path.relative_to(root))
            if path.is_symlink():
                target = path.resolve()
                assert target in targets, f"{relative} links to {target}"
                found[relative] = target
            elif path.is_file():
                assert not os.access(path, os.X_OK), f"{relative} is an executable file"
    return found


def install(path: Path, body: str) -> None:
    """Make ``path`` a fake that runs ``body``, replacing whatever was there."""
    body_file = body_path(path)
    body_file.parent.mkdir(parents=True, exist_ok=True)
    body_file.write_text(body)
    path.unlink(missing_ok=True)
    path.symlink_to(dispatcher())
