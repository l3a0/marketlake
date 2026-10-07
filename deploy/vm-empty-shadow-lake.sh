#!/bin/bash
# Marketlake: empty a shadow VM's lake so `bucket restore` can fill it.
#
# Usage, as root, with no arguments, after stopping every com.marketlake.* unit:
#
#     sudo deploy/vm-empty-shadow-lake.sh
#
# A shadow daemon writes into its lake from the first boot, and `bucket restore` refuses
# a lake that is not empty. This is the only deliberate delete of lake data in the
# restore steps, and after the cutover the same VM runs as primary, where emptying the
# lake would delete every minute since the last nightly upload. So it refuses unless all
# four of these hold:
#
#   1. the lake volume named by LAKE_VOLUME_ID in /etc/marketlake/bootstrap.conf is the
#      filesystem mounted at lake_root, proven by its UUID as vm-bootstrap.sh proves it;
#   2. the owner's ~/.config/marketlake/config.yaml sets role to exactly the string
#      shadow. An absent file, a file that does not parse and a missing key all refuse,
#      since an absent role means primary;
#   3. every loaded com.marketlake.* unit, timers included, is inactive or failed. A
#      waiting timer counts as active;
#   4. nothing is mounted below lake_root. rm --one-file-system stays out of a mount it
#      meets while descending, but it empties an entry it was handed that is itself a
#      mount point.
#
# It holds /run/marketlake-install.lock without waiting across the checks and the delete,
# so a bootstrap or a deploy cannot start units halfway through. Then it removes every
# entry in lake_root except lost+found, without crossing into another filesystem.
#
# It reads OWNER and LAKE_VOLUME_ID from bootstrap.conf, which the user_data shim wrote,
# and lake_root from config/vm.yaml, both as vm-bootstrap.sh reads them. A refusal is one
# `vm-empty-shadow-lake:` line on stderr and exit 2. Nothing is deleted on a refusal.
#
# MARKETLAKE_INSTALL_ROOT prefixes every system path this script reads or writes, as it
# does for vm-bootstrap.sh. It is for tests only, and is refused unless
# MARKETLAKE_INSTALL_TEST=1 is set.
#
# Written for bash 3.2 as well as a current bash, because the test suite runs it on a Mac.
set -euo pipefail

say() {
  echo "vm-empty-shadow-lake: $*"
}

refuse() {
  echo "vm-empty-shadow-lake: $*" >&2
  exit 2
}

if [[ $# -gt 0 ]]; then
  refuse "takes no arguments. Usage: vm-empty-shadow-lake.sh"
fi

ROOT="${MARKETLAKE_INSTALL_ROOT:-}"
if [[ -n "$ROOT" && "${MARKETLAKE_INSTALL_TEST:-}" != 1 ]]; then
  refuse "MARKETLAKE_INSTALL_ROOT is $ROOT, which only a test may set. Unset it."
fi
say "install root ${ROOT:-/}"

if [[ "$(id -u)" != 0 ]]; then
  refuse "run this as root, for example with sudo"
fi

# -- bootstrap.conf, parsed as vm-bootstrap.sh parses it --------------------------------

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
if [[ "${ENTRY%%:*}" != "$OWNER" ]]; then
  refuse "OWNER $OWNER is the uid of ${ENTRY%%:*}. Give the account name"
fi
OWNER_HOME="$(printf '%s\n' "$ENTRY" | cut -d: -f6)"
if [[ -z "$OWNER_HOME" ]]; then
  refuse "account $OWNER has no home directory"
fi

CHECKOUT="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null && pwd)"
LOCK="$ROOT/run/marketlake-install.lock"
FSTAB="$ROOT/etc/fstab"

# -- lake_root, read as vm-bootstrap.sh reads it ----------------------------------------

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
LAKE_ROOT_RE='^(/[A-Za-z0-9_.-]+)+$'
if [[ ! "$LAKE_ROOT" =~ $LAKE_ROOT_RE ]]; then
  refuse "lake_root in $VM_YAML must be an absolute path of letters, digits, '.', '_' and '-'"
fi
case "/$LAKE_ROOT/" in
  */./*|*/../*) refuse "lake_root in $VM_YAML must not hold a . or .. component" ;;
esac
MOUNT_DIR="$ROOT$LAKE_ROOT"

# -- the lock, held to the end ---------------------------------------------------------

mkdir -p "$(dirname "$LOCK")"
exec 9>"$LOCK"
if ! flock -n 9; then
  refuse "another run holds $LOCK, so nothing is deleted"
fi

# -- 1. the lake volume is what is mounted ---------------------------------------------

DEV="$ROOT/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_${VOLUME_ID/-/}"
if [[ ! -e "$DEV" ]]; then
  refuse "the lake volume $VOLUME_ID is not attached at $DEV, so nothing is deleted"
fi
UUID="$(blkid -p -s UUID -o value "$DEV")" || UUID=""
MOUNTED="$(findmnt -n -o UUID --mountpoint "$LAKE_ROOT")" || MOUNTED=""
SOURCE="$(findmnt --fstab --tab-file "$FSTAB" -n -o SOURCE --mountpoint "$LAKE_ROOT")" || SOURCE=""
# An empty UUID never matches, so two empty reads cannot pass.
if [[ -z "$UUID" || "$MOUNTED" != "$UUID" || "$SOURCE" != "UUID=$UUID" ]]; then
  refuse "the lake volume $VOLUME_ID is not the filesystem mounted at $LAKE_ROOT, so nothing is deleted"
fi

# -- 2. the role is shadow -------------------------------------------------------------

CONFIG="$OWNER_HOME/.config/marketlake/config.yaml"
if ! ROLE_CHECK="$(python3 -c '
import sys

import yaml

try:
    with open(sys.argv[1]) as handle:
        settings = yaml.safe_load(handle)
except FileNotFoundError:
    print("absent")
    sys.exit(0)
except Exception:
    print("unreadable")
    sys.exit(0)
if not isinstance(settings, dict) or "role" not in settings:
    print("no role")
elif isinstance(settings["role"], str) and settings["role"] == "shadow":
    print("shadow")
else:
    print("not shadow")
' "$CONFIG" 2>/dev/null)"; then
  refuse "could not read role from $CONFIG, so nothing is deleted"
fi
case "$ROLE_CHECK" in
  shadow) ;;
  absent) refuse "$CONFIG is missing, and no role means primary, so nothing is deleted" ;;
  unreadable) refuse "$CONFIG does not parse, so nothing is deleted" ;;
  "no role") refuse "$CONFIG sets no role, which means primary, so nothing is deleted" ;;
  *) refuse "$CONFIG does not set role to shadow, so nothing is deleted" ;;
esac

# -- 3. every unit is stopped ----------------------------------------------------------

if ! UNITS="$(systemctl list-units --all --no-legend --plain 'com.marketlake.*')"; then
  refuse "systemctl list-units failed, so nothing is deleted"
fi
RUNNING=""
while read -r unit _load active _ || [[ -n "${unit:-}" ]]; do
  if [[ -z "${unit:-}" ]]; then
    continue
  fi
  case "${active:-}" in
    inactive|failed) ;;
    *) RUNNING="$RUNNING $unit" ;;
  esac
done <<< "$UNITS"
if [[ -n "$RUNNING" ]]; then
  refuse "these units are not stopped:$RUNNING. Stop every com.marketlake.* unit first, so nothing is deleted"
fi

# -- 4. nothing is mounted below the lake root -----------------------------------------

# findmnt -R prints the lake root's own line, then one line for each mount below it.
if ! TARGETS="$(findmnt -n -R -o TARGET "$LAKE_ROOT")"; then
  refuse "findmnt could not list the mounts at $LAKE_ROOT, so nothing is deleted"
fi
if [[ "$TARGETS" != "$LAKE_ROOT" ]]; then
  refuse "something is mounted below $LAKE_ROOT, so nothing is deleted. Unmount it first"
fi

# -- the delete ------------------------------------------------------------------------

say "emptying $MOUNT_DIR, keeping lost+found"
find "$MOUNT_DIR" -mindepth 1 -maxdepth 1 ! -name lost+found -exec rm -rf --one-file-system -- {} +
say "done. Run bucket restore $LAKE_ROOT as $OWNER, then vm-bootstrap.sh"
