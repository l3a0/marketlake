"""Stand-ins for the disk and download tools the hosted VM's scripts call.

``deploy/vm-bootstrap.sh`` and ``deploy/vm-empty-shadow-lake.sh`` run here against these
fakes and the ones in ``tests.support.fake_systemd``, with ``MARKETLAKE_INSTALL_ROOT``
pointed at a temporary directory. The harness ``PATH`` is ``<bin>:/usr/bin:/bin``, so a
tool missing here would run for real. Every tool either script calls that could touch the
machine has a fake, including the ones a Mac has, such as ``chown``, ``cmp`` and ``curl``.

The fake disk is a directory, ``$STATE/disk``, so a test sets it up once and the fakes
change it the way the real tools would change a volume.

1. ``probe`` holds what ``blkid -p -o export`` prints. Its absence is a blank volume, on
   which ``blkid -p`` exits 2. ``BLKID_RC`` forces that probe's exit code.
2. ``uuid`` holds the filesystem UUID that ``blkid -p -s UUID -o value`` prints. Its
   absence makes that read exit 2 with no output, as a blank device does, so a script
   that reads the UUID before ``mkfs.ext4`` has run gets nothing.
3. ``mkfs.ext4`` writes both, with ``FAKE_NEW_UUID`` as the new filesystem's UUID.

``$STATE/mounted`` holds the UUID of whatever is mounted at the lake root. The fake
``systemctl start`` of a ``.mount`` unit writes it from the disk, and ``findmnt`` reads it.
``mount`` mounts the same way and exits 32 when something is already mounted, as
util-linux's does. ``$STATE/submounts`` lists the mount points below the lake root, which
``findmnt -R`` prints after the lake root's own line.

The other knobs: ``CMP_RC`` is ``cmp``'s exit on a volume with no probe result, since a
volume with one always differs from zeros. ``FINDMNT_VERIFY_RC`` is the exit of
``findmnt --verify`` on a file that holds an entry. On an empty or missing file it exits 1
whatever the knob says, so a test cannot pass against a check looser than the real one.
``FINDMNT_TREE_RC`` is the exit of ``findmnt -R``. ``FSTAB_SOURCE`` overrides what
``findmnt --fstab`` reads. ``UDEVADM_RC``, ``RESIZE2FS_RC``, ``MKFS_RC`` and ``CURL_RC``
fail those tools, and ``INSTALLER_RC`` fails the uv installer the fake ``curl`` writes. That
installer refuses to run, as the owner would fail to read it, unless its mode lets others
read it, and it logs the mode it found. ``CAT_FAIL`` names a file
the fake ``cat`` fails to read, as an I/O error would make it. ``$STATE/immutable`` lists
the directories ``chattr +i`` has marked.

Each fake logs its argv to ``$LOG``, except ``cat``, which the other fakes call too.
``python3`` runs the test's own interpreter, which has PyYAML, and logs only its first
argument, since the second is a whole program. ``chmod`` and ``rm`` log and then run the
real tool, inside the test root, and ``rm`` drops GNU's ``--one-file-system``, which a
Mac's ``rm`` does not know. ``sleep`` adds ``with fd 9 open`` to its line when it inherits
an open descriptor 9, which is the one the scripts lock the install lock through, so a
test sees a wait that holds the lock.
``mktemp`` with no template makes its file in ``$TMPDIR``, as GNU's does and a Mac's does
not, and logs the path it made.
"""

from __future__ import annotations

import sys
from pathlib import Path

from tests.support.fake_systemd import install_fakes

FAKE_UUID = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0"

FAKE_BLKID = r"""#!/bin/bash
printf 'blkid %s\n' "$*" >> "$LOG"
DISK="$STATE/disk"
case "$*" in
  *"-s UUID -o value"*)
    if [[ -f "$DISK/uuid" ]]; then cat "$DISK/uuid"; echo; exit 0; fi
    exit 2 ;;
  *"-o export"*)
    if [[ -n "${BLKID_RC:-}" ]]; then exit "$BLKID_RC"; fi
    if [[ -f "$DISK/probe" ]]; then cat "$DISK/probe"; exit 0; fi
    exit 2 ;;
esac
echo "fake blkid: unexpected arguments $*" >&2
exit 4
"""

FAKE_MKFS = r"""#!/bin/bash
printf 'mkfs.ext4 %s\n' "$*" >> "$LOG"
if [[ -n "${MKFS_RC:-}" ]]; then exit "$MKFS_RC"; fi
mkdir -p "$STATE/disk"
uuid="${FAKE_NEW_UUID:-11111111-2222-3333-4444-555555555555}"
printf 'DEVNAME=%s\nLABEL=marketlake\nUUID=%s\nTYPE=ext4\n' "${*: -1}" "$uuid" \
  > "$STATE/disk/probe"
printf '%s' "$uuid" > "$STATE/disk/uuid"
exit 0
"""

FAKE_CMP = r"""#!/bin/bash
printf 'cmp %s\n' "$*" >> "$LOG"
if [[ -f "$STATE/disk/probe" ]]; then exit 1; fi
exit "${CMP_RC:-0}"
"""

FAKE_FINDMNT = r"""#!/bin/bash
printf 'findmnt %s\n' "$*" >> "$LOG"
verify=0
fstab=0
submounts=0
tab=""
mountpoint=""
target=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --verify) verify=1 ;;
    --fstab) fstab=1 ;;
    -R) submounts=1 ;;
    --tab-file) tab="$2"; shift ;;
    --mountpoint) mountpoint="$2"; shift ;;
    -o) shift ;;
    -*) ;;
    *) target="$1" ;;
  esac
  shift
done
if [[ $verify == 1 ]]; then
  # A table with no entry at all is the one thing the fake always refuses.
  if [[ ! -s "$tab" ]]; then
    echo "fake findmnt: $tab holds no entry" >&2
    exit 1
  fi
  exit "${FINDMNT_VERIFY_RC:-0}"
fi
if [[ $submounts == 1 ]]; then
  # The target's own line, then a tree line for each mount below it, as -R prints them.
  if [[ -n "${FINDMNT_TREE_RC:-}" ]]; then exit "$FINDMNT_TREE_RC"; fi
  [[ -f "$STATE/mounted" ]] || exit 1
  echo "$target"
  if [[ -f "$STATE/submounts" ]]; then
    while IFS= read -r sub; do echo "└─$sub"; done < "$STATE/submounts"
  fi
  exit 0
fi
if [[ $fstab == 1 ]]; then
  if [[ -n "${FSTAB_SOURCE:-}" ]]; then echo "$FSTAB_SOURCE"; exit 0; fi
  [[ -f "$tab" ]] || exit 1
  awk -v mp="$mountpoint" '$1 !~ /^#/ && $2 == mp { print $1; found = 1 }
    END { exit found ? 0 : 1 }' "$tab"
  exit $?
fi
if [[ -f "$STATE/mounted" ]]; then
  cat "$STATE/mounted"
  echo
  exit 0
fi
exit 1
"""

FAKE_MOUNT = r"""#!/bin/bash
printf 'mount %s\n' "$*" >> "$LOG"
if [[ -f "$STATE/mounted" ]]; then
  echo "mount: ${*: -1}: already mounted." >&2
  exit 32
fi
cp "$STATE/disk/uuid" "$STATE/mounted"
exit 0
"""

FAKE_LSATTR = r"""#!/bin/bash
printf 'lsattr %s\n' "$*" >> "$LOG"
dir="${*: -1}"
if [[ ! -e "$dir" ]]; then
  echo "lsattr: No such file or directory while trying to stat $dir" >&2
  exit 1
fi
if [[ -f "$STATE/immutable" ]] && grep -qxF -- "$dir" "$STATE/immutable"; then
  echo "----i---------e------- $dir"
else
  echo "--------------e------- $dir"
fi
"""

FAKE_CHATTR = r"""#!/bin/bash
printf 'chattr %s\n' "$*" >> "$LOG"
if [[ "$1" == "+i" ]]; then echo "$2" >> "$STATE/immutable"; fi
exit 0
"""

FAKE_ESCAPE = r"""#!/bin/bash
printf 'systemd-escape %s\n' "$*" >> "$LOG"
path="${*: -1}"
path="${path#/}"
path="${path%/}"
echo "${path//\//-}.mount"
"""

FAKE_CURL = r"""#!/bin/bash
printf 'curl %s\n' "$*" >> "$LOG"
if [[ -n "${CURL_RC:-}" ]]; then exit "$CURL_RC"; fi
out=""
url=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -o) out="$2"; shift ;;
    https://*) url="$1" ;;
  esac
  shift
done
version="${url#https://astral.sh/uv/}"
version="${version%%/*}"
# The installer the URL names, as a script that writes a uv answering that version.
cat > "$out" <<EOF
mode="\$(ls -ln "\$0" | cut -c1-10)"
printf 'uv-installer mode %s\n' "\$mode" >> "\$LOG"
case "\$mode" in
  -??????r??) ;;
  *) echo "sh: \$0: Permission denied" >&2; exit 126 ;;
esac
printf 'uv-installer UV_NO_MODIFY_PATH=%s\n' "\$UV_NO_MODIFY_PATH" >> "\$LOG"
if [ -n "\${INSTALLER_RC:-}" ]; then exit "\$INSTALLER_RC"; fi
mkdir -p "\$FAKE_HOME/.local/bin"
rm -f "\$FAKE_HOME/.local/bin/uv"
printf '#!/bin/bash\necho "uv $version (fake 2026-01-01)"\n' > "\$FAKE_HOME/.local/bin/uv"
chmod 755 "\$FAKE_HOME/.local/bin/uv"
EOF
"""

# mktemp with no template makes its file in $TMPDIR, as GNU's does. A Mac's ignores
# $TMPDIR there, so a test checking the temporary directory would check nothing. The
# path is logged, so a test can find the file whatever made it.
FAKE_MKTEMP = r"""#!/bin/bash
if [[ $# -eq 0 ]]; then set -- "${TMPDIR:-/tmp}/tmp.XXXXXXXXXX"; fi
path="$(/usr/bin/mktemp "$@")" || exit $?
printf 'mktemp %s\n' "$path" >> "$LOG"
echo "$path"
"""

FAKE_LOGGED = """#!/bin/bash
line="{name} $*"
printf '%s\\n' "${{line% }}" >> "$LOG"
exit {rc}
"""

FAKE_CHMOD = """#!/bin/bash
printf 'chmod %s\\n' "$*" >> "$LOG"
exec /bin/chmod "$@"
"""

FAKE_RM = """#!/bin/bash
printf 'rm %s\\n' "$*" >> "$LOG"
args=()
for arg in "$@"; do
  if [[ "$arg" != --one-file-system ]]; then args+=("$arg"); fi
done
exec /bin/rm "${args[@]}"
"""

FAKE_SLEEP = """#!/bin/bash
held=""
if { : >&9; } 2>/dev/null; then held=" with fd 9 open"; fi
printf 'sleep %s%s\\n' "$*" "$held" >> "$LOG"
exit 0
"""

FAKE_CAT = """#!/bin/bash
if [[ -n "${CAT_FAIL:-}" && "${*: -1}" == "$CAT_FAIL" ]]; then
  echo "cat: $CAT_FAIL: Input/output error" >&2
  exit 1
fi
exec /bin/cat "$@"
"""

# A sequence of exit codes, one per call, read from a space-separated list. The last code
# repeats once the list runs out, and an empty list answers 0. The count lives in $STATE,
# so a sequence survives across processes.
NEXT_RC = r"""next_rc() {
  local name="$1" list="$2" n=0 i=0 rc=0 code
  if [[ -f "$STATE/count-$name" ]]; then n="$(<"$STATE/count-$name")"; fi
  printf '%s' "$((n + 1))" > "$STATE/count-$name"
  for code in $list; do
    rc="$code"
    if [[ $i == "$n" ]]; then break; fi
    i=$((i + 1))
  done
  echo "$rc"
}
"""

# The checkout's deploy/linux-install.sh, which test_control_plane_systemd.py runs for real.
FAKE_LINUX_INSTALL = (
    "#!/bin/bash\n"
    'printf \'linux-install %s\\n\' "$*" >> "$LOG"\n'
    + NEXT_RC
    + 'exit "$(next_rc install "${INSTALL_RCS:-}")"\n'
)

# The checkout's venv interpreter. The render and the roster read stdin, which the fake
# keeps so a test can check what each was fed.
FAKE_VENV_PYTHON = (
    "#!/bin/bash\n"
    'printf \'venv-python %s\\n\' "$*" >> "$LOG"\n'
    + NEXT_RC
    + r"""case "$2" in
  lake.vm_config) cat > "$STATE/stdin-render"; exit "$(next_rc render "${RENDER_RCS:-}")" ;;
  lake.token_store) exit "$(next_rc pull "${PULL_RCS:-}")" ;;
  lake.roster) cat > "$STATE/stdin-roster"; exit "$(next_rc roster "${ROSTER_RCS:-}")" ;;
esac
echo "fake venv python: unexpected module $2" >&2
exit 9
"""
)


def install_disk_fakes(bin_dir: Path) -> None:
    """Write the systemd fakes and every disk fake into ``bin_dir``, executable."""
    install_fakes(bin_dir)
    fakes = {
        "blkid": FAKE_BLKID,
        "mkfs.ext4": FAKE_MKFS,
        "cmp": FAKE_CMP,
        "findmnt": FAKE_FINDMNT,
        "mount": FAKE_MOUNT,
        "lsattr": FAKE_LSATTR,
        "chattr": FAKE_CHATTR,
        "systemd-escape": FAKE_ESCAPE,
        "curl": FAKE_CURL,
        "mktemp": FAKE_MKTEMP,
        "chown": FAKE_LOGGED.format(name="chown", rc=0),
        "sync": FAKE_LOGGED.format(name="sync", rc=0),
        "udevadm": FAKE_LOGGED.format(name="udevadm", rc='"${UDEVADM_RC:-0}"'),
        "resize2fs": FAKE_LOGGED.format(name="resize2fs", rc='"${RESIZE2FS_RC:-0}"'),
        "chmod": FAKE_CHMOD,
        "rm": FAKE_RM,
        "sleep": FAKE_SLEEP,
        "cat": FAKE_CAT,
        "python3": (
            f'#!/bin/bash\nprintf \'python3 %s\\n\' "$1" >> "$LOG"\nexec {sys.executable} "$@"\n'
        ),
    }
    for name, body in fakes.items():
        path = bin_dir / name
        path.write_text(body)
        path.chmod(0o755)
