#!/bin/bash
# Marketlake: install or update the control plane on a Linux host that runs systemd.
#
# Usage, as root, from any directory:
#
#     sudo deploy/linux-install.sh --owner <account> --lake-mount <path> [--config <path>]
#         [--sync-only]
#
# The host's first-boot setup and the deploy job both call this, and nothing else
# installs. It is safe to run again on a host that already has it, and every run without
# --sync-only renders the units afresh from this checkout. In order, it:
#
#   1. refuses unless it runs as root, --owner names an account by its name rather
#      than its uid, and --lake-mount is given. The flag is required because a run
#      without --sync-only re-renders, so a run that left it out would strip the units'
#      wait for the lake's volume;
#   2. takes /run/marketlake-install.lock, waiting up to 600 seconds, and holds it to
#      the end, so two runs cannot interleave and leave the older commit installed;
#   3. as the owner, runs uv sync in this checkout. With --sync-only it stops here;
#   4. as the owner, renders into a fresh systemd.new beside
#      <owner home>/.local/state/marketlake/systemd and swaps it into place;
#   5. runs the rendered install.sh, which copies the units, reloads systemd, and
#      enables and starts the residents and the timers.
#
# Every write under the owner's home or this checkout runs as the owner, so no
# root-owned file lands in the owner's tree. Root then runs a script and copies units the
# owner's account wrote, so the owner account is trusted with root on this host.
#
# Each step prints a `linux-install: <step>` line first, so a caller reading the log can
# tell which step a nonzero exit came from. A refusal is one `linux-install:` line on
# stderr and exit 2.
#
# --sync-only is for deploy/vm-bootstrap.sh. The bootstrap needs the venv to render
# config.yaml, and it installs the units only after that, so the residents never start
# before their config exists. With the flag, this prints `linux-install: done` right
# after the sync, and renders and starts nothing.
#
# MARKETLAKE_INSTALL_ROOT prefixes every system path this and install.sh write, the lock
# included. It is for tests only, and is refused unless MARKETLAKE_INSTALL_TEST=1 is set.
#
# Written for bash 3.2 as well as a current bash, because the test suite runs it on a Mac.
set -euo pipefail

say() {
  echo "linux-install: $*"
}

refuse() {
  echo "linux-install: $*" >&2
  exit 2
}

OWNER=""
LAKE_MOUNT=""
CONFIG=""
SYNC_ONLY=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --owner|--lake-mount|--config)
      if [[ $# -lt 2 || -z "$2" ]]; then
        refuse "$1 needs a value"
      fi
      case "$1" in
        --owner) OWNER="$2" ;;
        --lake-mount) LAKE_MOUNT="$2" ;;
        --config) CONFIG="$2" ;;
      esac
      shift 2 ;;
    --sync-only)
      SYNC_ONLY=1
      shift ;;
    *)
      refuse "unknown argument $1. Usage: linux-install.sh --owner <account> --lake-mount <path> [--config <path>] [--sync-only]. Only vm-bootstrap.sh passes --sync-only" ;;
  esac
done

ROOT="${MARKETLAKE_INSTALL_ROOT:-}"
if [[ -n "$ROOT" && "${MARKETLAKE_INSTALL_TEST:-}" != 1 ]]; then
  refuse "MARKETLAKE_INSTALL_ROOT is $ROOT, which only a test may set. Unset it."
fi
say "install root ${ROOT:-/}"

if [[ "$(id -u)" != 0 ]]; then
  refuse "run this as root, for example with sudo"
fi
if [[ -z "$OWNER" ]]; then
  refuse "--owner is required: the account the jobs run as"
fi
if [[ -z "$LAKE_MOUNT" ]]; then
  refuse "--lake-mount is required: the lake's mount point, or lake_root when the lake sits on the root volume"
fi
if ! ENTRY="$(getent passwd "$OWNER")"; then
  refuse "--owner $OWNER names no account on this host"
fi
# getent resolves a uid as well as a name, while sudo -u reads a bare number as a name.
# So a uid would pass here and fail later, at the first step run as the owner.
if [[ "${ENTRY%%:*}" != "$OWNER" ]]; then
  refuse "--owner $OWNER is the uid of ${ENTRY%%:*}. Give the account name"
fi
OWNER_HOME="$(printf '%s\n' "$ENTRY" | cut -d: -f6)"
if [[ -z "$OWNER_HOME" ]]; then
  refuse "account $OWNER has no home directory"
fi

# An exported CDPATH would make cd search other directories and print the one it
# chose into this substitution. Emptying it for the one cd keeps the path exact.
CHECKOUT="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null && pwd)"
PYTHON="$CHECKOUT/.venv/bin/python"
STATE_DIR="$OWNER_HOME/.local/state/marketlake"
LIVE="$STATE_DIR/systemd"
NEW="$STATE_DIR/systemd.new"
OLD="$STATE_DIR/systemd.old"
LOCK="$ROOT/run/marketlake-install.lock"

as_owner() {
  sudo -u "$OWNER" -H "$@"
}

cd "$CHECKOUT"

say "taking the install lock $LOCK"
mkdir -p "$(dirname "$LOCK")"
exec 9>"$LOCK"
if ! flock -w 600 9; then
  refuse "another install held $LOCK for 600 seconds, so this one gave up"
fi

# uv by absolute path, because sudo's secure_path leaves ~/.local/bin out.
say "syncing the environment in $CHECKOUT as $OWNER"
as_owner "$OWNER_HOME/.local/bin/uv" sync --frozen --no-dev
if [[ $SYNC_ONLY == 1 ]]; then
  say "done"
  exit 0
fi

# A killed run can leave either directory behind, and the render must start empty.
say "rendering the units into $NEW as $OWNER"
as_owner rm -rf "$NEW" "$OLD"
as_owner mkdir -p "$STATE_DIR"
# bash 3.2 treats an empty array as unbound under set -u, hence the guarded expansion.
EXTRA=()
if [[ -n "$CONFIG" ]]; then
  EXTRA=(--config "$CONFIG")
fi
as_owner "$PYTHON" -m lake.control_plane render --init systemd --out "$NEW" \
  --python "$PYTHON" --project-dir "$CHECKOUT" --home "$OWNER_HOME" --owner "$OWNER" \
  --lake-mount "$LAKE_MOUNT" ${EXTRA[@]+"${EXTRA[@]}"}

# A rename onto a non-empty directory fails, so the old one moves aside first. A fresh
# directory also drops any file the render no longer names, and never rewrites a script
# under a bash that is running it.
say "swapping $NEW into $LIVE"
if [[ -e "$LIVE" ]]; then
  as_owner mv "$LIVE" "$OLD"
fi
as_owner mv "$NEW" "$LIVE"
as_owner rm -rf "$OLD"

say "running $LIVE/install.sh"
"$LIVE/install.sh"
say "done"
