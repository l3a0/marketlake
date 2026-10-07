#!/bin/bash
# Marketlake: bring the hosted VM from a fresh boot to a running daemon, with no login.
#
# Usage, as root, with no arguments:
#
#     sudo deploy/vm-bootstrap.sh
#
# cloud-init's user_data shim runs this on the first boot, after it writes
# /etc/marketlake/bootstrap.conf and clones the repository as the owner. The owner can run
# it again by hand over SSH. Each step skips work that is already done, so a second run
# under the same conditions changes nothing. In order, it:
#
#   1. refuses unless it runs as root, and reads OWNER and LAKE_VOLUME_ID from
#      bootstrap.conf. The file is parsed, never sourced, so a stray line cannot run;
#   2. reads lake_root from config/vm.yaml with the image's python3 and PyYAML, so the
#      mount point has one source;
#   3. waits a bounded time for the lake volume's by-id link, since AWS attaches the
#      volume after the instance starts;
#   4. when nothing is mounted at lake_root, makes the bare directory immutable, so
#      nothing can write the lake onto the root volume, and formats the volume only when
#      two checks prove it blank;
#   5. writes the fstab line for the volume, keyed on the mount point, atomically;
#   6. mounts it through systemctl start of its mount unit, which runs e2fsck first;
#   7. proves the right volume is mounted by its UUID, and only then chowns the lake root
#      to the owner and grows the filesystem to the volume's size;
#   8. installs the pinned uv as the owner when the installed one differs;
#   9. runs deploy/linux-install.sh, retrying a transient failure;
#  10. after the install returns, renders config.yaml, pulls the Schwab token and
#      applies the roster, each as the owner and in that order. Each attempt holds the
#      install lock, and the wait before a retry does not.
#
# Every disk step and the install stop the run at once with one line, because a later
# step on a wrong disk would write the lake where nothing keeps it. A failure in step 10
# prints one line, skips only the steps that need what failed, and makes the run exit 1
# at the end. A refusal, when the script declines on purpose, exits 2. No line prints a
# config value, and the script never runs with set -x.
#
# docs/design.md and issue #686 carry the reasoning for each step.
#
# MARKETLAKE_INSTALL_ROOT prefixes every system path this script reads or writes:
# bootstrap.conf, /etc/fstab, /dev/disk/by-id, the mount-point directory and the lock.
# Names that live inside a table, such as the fstab line's mount point and the mount
# unit's name, stay unprefixed. It is for tests only, and is refused unless
# MARKETLAKE_INSTALL_TEST=1 is set.
#
# Written for bash 3.2 as well as a current bash, because the test suite runs it on a Mac.
set -euo pipefail

say() {
  echo "vm-bootstrap: $*"
}

refuse() {
  echo "vm-bootstrap: $*" >&2
  exit 2
}

stop() {
  echo "vm-bootstrap: $*" >&2
  exit 1
}

FAILED=0
fail() {
  echo "vm-bootstrap: $*" >&2
  FAILED=1
}

if [[ $# -gt 0 ]]; then
  refuse "takes no arguments. Usage: vm-bootstrap.sh"
fi

ROOT="${MARKETLAKE_INSTALL_ROOT:-}"
if [[ -n "$ROOT" && "${MARKETLAKE_INSTALL_TEST:-}" != 1 ]]; then
  refuse "MARKETLAKE_INSTALL_ROOT is $ROOT, which only a test may set. Unset it."
fi
say "install root ${ROOT:-/}"

if [[ "$(id -u)" != 0 ]]; then
  refuse "run this as root, for example with sudo"
fi

# -- bootstrap.conf --------------------------------------------------------------------

CONF="$ROOT/etc/marketlake/bootstrap.conf"
if [[ ! -f "$CONF" ]]; then
  refuse "$CONF is missing. The user_data shim writes it"
fi
OWNER=""
VOLUME_ID=""
while IFS= read -r line || [[ -n "$line" ]]; do
  if [[ -z "$line" ]]; then
    continue
  fi
  key="${line%%=*}"
  value="${line#*=}"
  if [[ "$key" == "$line" ]]; then
    refuse "$CONF holds a line that is not KEY=VALUE"
  fi
  case "$key" in
    OWNER)
      if [[ -n "$OWNER" ]]; then refuse "$CONF sets OWNER twice"; fi
      OWNER="$value" ;;
    LAKE_VOLUME_ID)
      if [[ -n "$VOLUME_ID" ]]; then refuse "$CONF sets LAKE_VOLUME_ID twice"; fi
      VOLUME_ID="$value" ;;
    *)
      refuse "$CONF holds an unknown key. It takes only OWNER and LAKE_VOLUME_ID" ;;
  esac
done < "$CONF"
# The same account rule control_plane applies to --owner, so a name this accepts is
# one the install accepts too.
ACCOUNT_RE='^[A-Za-z_][A-Za-z0-9_.-]*$'
if [[ ! "$OWNER" =~ $ACCOUNT_RE ]]; then
  refuse "$CONF must set OWNER to an account name"
fi
VOLUME_RE='^vol-[0-9a-f]+$'
if [[ ! "$VOLUME_ID" =~ $VOLUME_RE ]]; then
  refuse "$CONF must set LAKE_VOLUME_ID to a volume id, vol- and hex digits"
fi
if ! ENTRY="$(getent passwd "$OWNER")"; then
  refuse "OWNER $OWNER names no account on this host"
fi
# getent resolves a uid as well as a name, while sudo -u reads a bare number as a name.
if [[ "${ENTRY%%:*}" != "$OWNER" ]]; then
  refuse "OWNER $OWNER is the uid of ${ENTRY%%:*}. Give the account name"
fi
OWNER_HOME="$(printf '%s\n' "$ENTRY" | cut -d: -f6)"
if [[ -z "$OWNER_HOME" ]]; then
  refuse "account $OWNER has no home directory"
fi

# An exported CDPATH would make cd search other directories and print the one it
# chose into this substitution. Emptying it for the one cd keeps the path exact.
CHECKOUT="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null && pwd)"
PYTHON="$CHECKOUT/.venv/bin/python"
LOCK="$ROOT/run/marketlake-install.lock"
FSTAB="$ROOT/etc/fstab"

as_owner() {
  sudo -u "$OWNER" -H "$@"
}

# -- lake_root -------------------------------------------------------------------------

# The image's python3, not the venv's, because the venv does not exist until the install
# has run, and the install needs the mount point first. cloud-init depends on PyYAML, so
# the image always has it.
VM_YAML="$CHECKOUT/config/vm.yaml"
if ! LAKE_ROOT="$(python3 -c '
import sys

import yaml

with open(sys.argv[1]) as handle:
    settings = yaml.safe_load(handle)
value = settings.get("lake_root") if isinstance(settings, dict) else None
if not isinstance(value, str):
    sys.exit(2)
print(value)
' "$VM_YAML" 2>/dev/null)"; then
  refuse "could not read a lake_root string from $VM_YAML"
fi
# Absolute, with plain path characters only. The install refuses quotes, whitespace, %
# and $, and the fstab line and the mount unit's name cannot carry them either.
LAKE_ROOT_RE='^(/[A-Za-z0-9_.-]+)+$'
if [[ ! "$LAKE_ROOT" =~ $LAKE_ROOT_RE ]]; then
  refuse "lake_root in $VM_YAML must be an absolute path of letters, digits, '.', '_' and '-'"
fi
case "/$LAKE_ROOT/" in
  */./*|*/../*) refuse "lake_root in $VM_YAML must not hold a . or .. component" ;;
esac
MOUNT_DIR="$ROOT$LAKE_ROOT"

# -- the lake volume -------------------------------------------------------------------

# AWS sets the NVMe serial to the volume id without its dash, and Ubuntu's udev rules
# build the by-id link from the model and that serial.
DEV="$ROOT/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_${VOLUME_ID/-/}"
say "waiting for the lake volume $VOLUME_ID at $DEV"
tries=0
while [[ ! -e "$DEV" ]]; do
  tries=$((tries + 1))
  if [[ $tries -gt 60 ]]; then
    stop "the lake volume $VOLUME_ID did not appear at $DEV within 300 seconds"
  fi
  sleep 5
done

# findmnt exits 0 when something is mounted at the path, whatever it is, and 1 when
# nothing is. A mounted filesystem with no UUID still counts as mounted, so the proof
# below refuses it rather than the guard formatting underneath it.
if findmnt -n -o UUID --mountpoint "$LAKE_ROOT" >/dev/null; then
  # A chattr +i here would land on the mounted volume's own root inode, where the flag
  # persists on disk and the kernel refuses every write into the lake. So the mounted
  # branch never touches the directory, and never formats.
  say "something is mounted at $LAKE_ROOT, so the directory and the volume are left as they are"
else
  if [[ -e "$MOUNT_DIR" ]]; then
    if ! FLAGS="$(lsattr -d "$MOUNT_DIR")"; then
      stop "could not read the attributes of $MOUNT_DIR"
    fi
    FLAGS="${FLAGS%% *}"
  else
    FLAGS=""
  fi
  case "$FLAGS" in
    *i*)
      say "$MOUNT_DIR is already immutable" ;;
    *)
      # The bare directory stays root's and immutable, so a service that starts without
      # its mount can write nothing onto the root volume.
      say "making $MOUNT_DIR an immutable root:root 0755 directory"
      mkdir -p "$MOUNT_DIR" || stop "could not create $MOUNT_DIR"
      chown root:root "$MOUNT_DIR" || stop "could not chown $MOUNT_DIR to root"
      chmod 0755 "$MOUNT_DIR" || stop "could not chmod $MOUNT_DIR"
      chattr +i "$MOUNT_DIR" || stop "could not make $MOUNT_DIR immutable" ;;
  esac

  # The guard has three outcomes. An existing ext4 filesystem mounts as it is, which is
  # every replacement's path. A volume proven blank by two checks is formatted.
  # Anything else stops. blkid -p also exits 2 when a probe read fails, so a read error
  # on a lake volume would look blank to it alone. cmp reads the first MiB, where the
  # ext4 superblock, an MBR and a GPT header all sit, and exits 1 on a non-zero byte and
  # 2 on a read error.
  probe_rc=0
  PROBE="$(blkid -p -o export "$DEV")" || probe_rc=$?
  TYPE=""
  while IFS= read -r line; do
    case "$line" in
      TYPE=*) TYPE="${line#TYPE=}" ;;
    esac
  done <<< "$PROBE"
  if [[ $probe_rc == 0 && "$TYPE" == ext4 ]]; then
    say "the lake volume holds an ext4 filesystem, so it mounts without formatting"
  elif [[ $probe_rc == 2 ]]; then
    cmp_rc=0
    cmp -s -n 1048576 "$DEV" /dev/zero || cmp_rc=$?
    if [[ $cmp_rc != 0 ]]; then
      refuse "blkid found no filesystem on $DEV, but its first MiB is not all zero (cmp exit $cmp_rc), so it is not formatted"
    fi
    # Never -F. mkfs.ext4's own check for an existing filesystem asks only on a
    # terminal, so under cloud-init it would format silently. Without -F it still
    # refuses a mounted or busy device.
    say "the lake volume is blank, so it gets an ext4 filesystem"
    mkfs.ext4 -q -m 0 -L marketlake "$DEV" || stop "mkfs.ext4 failed on $DEV"
    # The fsck unit binds to the by-uuid link, which udev creates after mkfs.
    udevadm settle || stop "udevadm settle failed after mkfs.ext4"
  else
    refuse "the lake volume at $DEV holds something other than ext4 (blkid exit $probe_rc), so it is not formatted"
  fi
fi

# The UUID comes from the device the volume id names, on every run and in both branches.
# The fstab line and the proof use it, never fstab's own. blkid -p opens the device
# read-only without O_EXCL, so it works on a mounted device.
UUID="$(blkid -p -s UUID -o value "$DEV")" || stop "could not read the UUID of $DEV"
if [[ -z "$UUID" ]]; then
  refuse "the lake volume at $DEV has no filesystem UUID"
fi

# -- fstab -----------------------------------------------------------------------------

# The line is keyed on the mount point, so a rerun replaces a stale line rather than
# adding a second one. The 2 lets e2fsck -p keep a damaged filesystem unmounted.
LINE="UUID=$UUID $LAKE_ROOT ext4 defaults,nofail 0 2"
CURRENT=""
if [[ -f "$FSTAB" ]]; then
  # The x keeps any trailing newline, which a command substitution would strip. The &&
  # makes a failed read fail the substitution. Joined by ; the status would be printf's,
  # and an unread fstab would be rewritten holding only the lake's line.
  if ! CURRENT="$(cat -- "$FSTAB" && printf x)"; then
    stop "could not read $FSTAB, so it is unchanged"
  fi
  CURRENT="${CURRENT%x}"
fi
DESIRED=""
while IFS= read -r line || [[ -n "$line" ]]; do
  read -r first second _ <<< "$line" || true
  case "${first:-}" in
    \#*) ;;
    *)
      if [[ "${second:-}" == "$LAKE_ROOT" ]]; then
        continue
      fi ;;
  esac
  DESIRED="$DESIRED$line"$'\n'
done <<< "${CURRENT%$'\n'}"
if [[ -z "${CURRENT%$'\n'}" ]]; then
  DESIRED=""
fi
DESIRED="$DESIRED$LINE"$'\n'
# findmnt --verify accepts a file holding only the lake's line, so this check is what
# keeps a rewrite from dropping the root entry and the next boot with it.
HAS_ROOT=0
while IFS= read -r line; do
  read -r first second _ <<< "$line" || true
  case "${first:-}" in
    \#*) ;;
    *)
      if [[ "${second:-}" == / ]]; then
        HAS_ROOT=1
      fi ;;
  esac
done <<< "$DESIRED"
if [[ $HAS_ROOT == 0 ]]; then
  refuse "$FSTAB has no entry for /, so it is not rewritten and nothing is installed"
fi
if [[ "$DESIRED" == "$CURRENT" ]]; then
  say "$FSTAB already mounts the lake volume at $LAKE_ROOT"
else
  # The root entry lives in the same file, so a truncated write could leave the next
  # boot without its root mount. The new file is checked, synced, then renamed over.
  say "writing the lake volume's line into $FSTAB"
  TMP="$(mktemp "$ROOT/etc/fstab.XXXXXX")" || stop "could not create a temporary file beside $FSTAB"
  printf '%s' "$DESIRED" > "$TMP" || { rm -f -- "$TMP"; stop "could not write $TMP"; }
  chmod 0644 "$TMP" || { rm -f -- "$TMP"; stop "could not chmod $TMP"; }
  if ! findmnt --verify --tab-file "$TMP" >/dev/null; then
    rm -f -- "$TMP"
    stop "findmnt --verify refused the new fstab, so $FSTAB is unchanged"
  fi
  sync || { rm -f -- "$TMP"; stop "sync failed before replacing $FSTAB"; }
  mv -f -- "$TMP" "$FSTAB" || { rm -f -- "$TMP"; stop "could not replace $FSTAB"; }
  systemctl daemon-reload || stop "systemctl daemon-reload failed after writing $FSTAB"
fi

# -- mount, prove, chown, grow ---------------------------------------------------------

# Never a plain mount. The mount unit requires systemd-fsck@, so starting it runs
# e2fsck -p first, and starting an active unit succeeds where a second mount exits 32.
UNIT="$(systemd-escape -p --suffix=mount "$LAKE_ROOT")" || stop "systemd-escape failed for $LAKE_ROOT"
say "starting $UNIT"
systemctl start "$UNIT" || stop "systemctl start $UNIT failed, so nothing is installed"

# systemctl start on an active unit succeeds whatever is mounted, so the UUID is the only
# proof that the lake volume is the one there. An empty UUID never matches.
MOUNTED="$(findmnt -n -o UUID --mountpoint "$LAKE_ROOT")" || MOUNTED=""
if [[ -z "$MOUNTED" || "$MOUNTED" != "$UUID" ]]; then
  refuse "the filesystem mounted at $LAKE_ROOT is not the lake volume $VOLUME_ID, so nothing is installed"
fi
SOURCE="$(findmnt --fstab --tab-file "$FSTAB" -n -o SOURCE --mountpoint "$LAKE_ROOT")" || SOURCE=""
if [[ "$SOURCE" != "UUID=$UUID" ]]; then
  refuse "$FSTAB does not mount the lake volume $VOLUME_ID at $LAKE_ROOT, so nothing is installed"
fi

# mkfs leaves the root root:root 0755, so without this every write by the daemon fails.
# Not recursive: lost+found stays root's.
chown "$OWNER": "$MOUNT_DIR" || stop "could not chown $MOUNT_DIR to $OWNER"
# A larger volume does not grow ext4 on its own. Never -f: without it resize2fs refuses
# to shrink a mounted filesystem, and prints that there is nothing to do when the sizes
# already match.
resize2fs "$DEV" || stop "resize2fs failed on $DEV, so nothing is installed"

# -- uv --------------------------------------------------------------------------------

UV="$OWNER_HOME/.local/bin/uv"
UV_VERSION=""
if [[ -f "$CHECKOUT/.tool-versions" ]]; then
  while read -r name version _ || [[ -n "${name:-}" ]]; do
    if [[ "${name:-}" == uv ]]; then
      UV_VERSION="${version:-}"
    fi
  done < "$CHECKOUT/.tool-versions"
fi
VERSION_RE='^[0-9]+\.[0-9]+\.[0-9]+$'
if [[ ! "$UV_VERSION" =~ $VERSION_RE ]]; then
  refuse "$CHECKOUT/.tool-versions must pin uv as a line 'uv <x.y.z>'"
fi
HAVE=""
if [[ -x "$UV" ]]; then
  HAVE="$(as_owner "$UV" --version 2>/dev/null)" || HAVE=""
fi
read -r _ HAVE _ <<< "$HAVE" || true
if [[ "${HAVE:-}" == "$UV_VERSION" ]]; then
  say "uv $UV_VERSION is already installed"
else
  # Downloaded first rather than piped into sh, so a cut-off download never runs half a
  # script. sudo's env_reset would drop UV_NO_MODIFY_PATH set before sudo, so env sets it
  # after. --retry alone skips a failed DNS lookup (exit 6) and a refused connection
  # (exit 7), and curl 8.5 has no flag for DNS alone, so --retry-all-errors retries
  # every failure, still at most 5 times.
  say "installing uv $UV_VERSION as $OWNER"
  INSTALLER="$(mktemp)" || stop "could not create a temporary file for the uv installer"
  if ! curl --proto '=https' --tlsv1.2 -fsSL --retry 5 --retry-all-errors -o "$INSTALLER" \
      "https://astral.sh/uv/$UV_VERSION/install.sh"; then
    rm -f -- "$INSTALLER"
    stop "could not download the uv $UV_VERSION installer, so nothing is installed"
  fi
  # mktemp makes the file 0600 and root's, and the owner runs it.
  chmod 0644 "$INSTALLER" || { rm -f -- "$INSTALLER"; stop "could not chmod $INSTALLER"; }
  if ! as_owner env UV_NO_MODIFY_PATH=1 sh "$INSTALLER"; then
    rm -f -- "$INSTALLER"
    stop "the uv $UV_VERSION installer failed, so nothing is installed"
  fi
  rm -f -- "$INSTALLER"
fi

# -- the install -----------------------------------------------------------------------

# Exit 1 is retried, since a refused proxy during uv sync exits 1. The install's own
# refusals exit 2, which no retry fixes.
INSTALL_TRIES=5
attempt=1
while :; do
  rc=0
  "$CHECKOUT/deploy/linux-install.sh" --owner "$OWNER" --lake-mount "$LAKE_ROOT" || rc=$?
  if [[ $rc == 0 ]]; then
    break
  fi
  if [[ $rc != 1 ]]; then
    stop "deploy/linux-install.sh exited $rc, a refusal, so the bootstrap stops"
  fi
  if [[ $attempt -ge $INSTALL_TRIES ]]; then
    stop "deploy/linux-install.sh exited 1 on all $INSTALL_TRIES attempts, so the bootstrap stops"
  fi
  say "deploy/linux-install.sh exited 1, retrying in 30 seconds (attempt $attempt of $INSTALL_TRIES)"
  sleep 30
  attempt=$((attempt + 1))
done

# -- config, token, roster -------------------------------------------------------------

# Exits 1 and 3 are retried: 3 is credentials the metadata service does not serve yet,
# and 1 covers a network failure and an AccessDenied while a fresh IAM change spreads.
# Exit 2 is a refusal.
#
# The lock is taken around each attempt, never across the retries, and the 20-second
# wait runs without it. One attempt against an endpoint that hangs takes about 120
# seconds: three botocore attempts, each a 10-second connect timeout and a 30-second
# read timeout. So the lock is held about 120 seconds at a time, under the 600 seconds
# another install waits for it with flock -w 600. Held across six tries of both steps,
# it could stay taken about 1,640 seconds.
#
# The lock is taken only after the install returns, because the install takes the same
# lock with flock -w 600, so holding it across the install would make the install wait
# ten minutes and refuse.
STEP_TRIES=6
LOCK_WAIT=600
mkdir -p "$(dirname "$LOCK")"

# Runs one command holding the install lock, with stdin from the file the first argument
# names, or the script's own stdin when it is empty. Closing the descriptor releases the
# lock. HELD reads 0 when another run held the lock for the whole wait, and the command
# never ran.
HELD=0
locked() {
  local input="$1" rc=0
  shift
  HELD=0
  exec 9>"$LOCK" || stop "could not open the install lock $LOCK"
  if ! flock -w "$LOCK_WAIT" 9; then
    exec 9>&-
    return 1
  fi
  HELD=1
  if [[ -n "$input" ]]; then
    "$@" < "$input" || rc=$?
  else
    "$@" || rc=$?
  fi
  exec 9>&-
  return "$rc"
}

retried() {
  local label="$1" input="$2" attempt=1 rc
  shift 2
  while :; do
    rc=0
    locked "$input" as_owner "$PYTHON" "$@" || rc=$?
    if [[ $HELD == 0 ]]; then
      fail "another run held $LOCK for $LOCK_WAIT seconds, so $label is skipped"
      return 1
    fi
    case "$rc" in
      0)
        return 0 ;;
      1|3)
        if [[ $attempt -ge $STEP_TRIES ]]; then
          fail "$label exited $rc on all $STEP_TRIES attempts"
          return 1
        fi
        say "$label exited $rc, retrying in 20 seconds (attempt $attempt of $STEP_TRIES)"
        sleep 20
        attempt=$((attempt + 1)) ;;
      *)
        fail "$label exited $rc, which is not retried"
        return 1 ;;
    esac
  done
}

say "rendering config.yaml as $OWNER, holding the install lock $LOCK"
if retried "the config render" "$VM_YAML" -m lake.vm_config render; then
  # The pull reads config.yaml, so it runs only after a render that succeeded.
  say "pulling the Schwab token as $OWNER"
  retried "the token pull" "" -m lake.token_store pull \
    --token "$OWNER_HOME/.config/marketlake/token.json" || true
  # The roster needs config.yaml but not the token. A refusal, such as a primary whose
  # lake is not restored yet, prints its own line and leaves the rest of the run alone.
  say "applying the roster as $OWNER"
  roster_rc=0
  locked "$CHECKOUT/config/tickers.yaml" as_owner "$PYTHON" -m lake.roster apply \
    || roster_rc=$?
  if [[ $HELD == 0 ]]; then
    fail "another run held $LOCK for $LOCK_WAIT seconds, so the roster apply is skipped"
  elif [[ $roster_rc != 0 ]]; then
    fail "the roster apply exited $roster_rc"
  fi
else
  fail "the token pull and the roster are skipped, because both read config.yaml"
fi

if [[ $FAILED != 0 ]]; then
  stop "finished with a failed step, listed above"
fi
say "done"
