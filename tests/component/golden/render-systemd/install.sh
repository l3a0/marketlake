#!/bin/bash
# Marketlake control plane on a systemd host: install or update the units.
#
# Written by `python -m lake.control_plane render --init systemd`, which never runs
# it. deploy/linux-install.sh renders this directory afresh and runs this script,
# as root, on every install. Running it again by hand is safe:
#
#     sudo ./install.sh
#
# It copies a unit only when its content changed, and prints `changed: <file>`
# when it does. It retires any com.marketlake unit the render no longer names,
# reloads systemd, and enables and starts the residents and the timers. Starting
# skips what already runs, so a running service keeps its old definition until
# its next restart. ./restart.sh is how to give it the new one.
#
# It closes by reading back each resident's state. A resident that restarts every
# 10 seconds is expected until config.yaml and tickers.yaml land.
# Every command is echoed before it runs.
set -euo pipefail

if [[ "$(id -u)" != 0 ]]; then
  echo "install.sh: run this as root, for example with sudo" >&2
  exit 2
fi
ROOT="${MARKETLAKE_INSTALL_ROOT:-}"
if [[ -n "$ROOT" && "${MARKETLAKE_INSTALL_TEST:-}" != 1 ]]; then
  echo "install.sh: MARKETLAKE_INSTALL_ROOT is $ROOT, which only a test may set." >&2
  echo "install.sh: Unset it to act on the real system paths." >&2
  exit 2
fi
echo "install.sh: install root ${ROOT:-/}"
UNIT_DIR="$ROOT/etc/systemd/system"
NEEDRESTART_DIR="$ROOT/etc/needrestart/conf.d"
STAMP_DIR="$ROOT/var/lib/systemd/timers"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

UNITS=(com.marketlake.daemon.service com.marketlake.dashboard.service com.marketlake.self-check.service com.marketlake.self-check.timer com.marketlake.calendar-probe.service com.marketlake.calendar-probe.timer com.marketlake.sunday.service com.marketlake.sunday.timer com.marketlake.eod-sweep.service com.marketlake.eod-sweep.timer)
ENABLE=(com.marketlake.daemon.service com.marketlake.dashboard.service com.marketlake.self-check.timer com.marketlake.calendar-probe.timer com.marketlake.sunday.timer com.marketlake.eod-sweep.timer)
RESIDENTS=(com.marketlake.daemon.service com.marketlake.dashboard.service)

rendered() {
  local unit
  for unit in "${UNITS[@]}"; do
    if [[ "$unit" == "$1" ]]; then return 0; fi
  done
  return 1
}

# Copy $1 to $2 through a temporary file and a rename, only when the two differ.
place() {
  local src="$1" dest="$2" tmp
  if [[ -f "$dest" ]] && cmp -s "$src" "$dest"; then
    return 0
  fi
  tmp="${dest%/*}/.${dest##*/}.tmp"
  echo "+ install -m 644 $src $tmp"
  install -m 644 "$src" "$tmp"
  echo "+ mv -f $tmp $dest"
  mv -f "$tmp" "$dest"
  echo "changed: ${dest##*/}"
}

# 1. Copy the units and the needrestart drop-in. The drop-in's directory is created
# because a host without needrestart has none.
echo "+ mkdir -p $UNIT_DIR $NEEDRESTART_DIR"
mkdir -p "$UNIT_DIR" "$NEEDRESTART_DIR"
for unit in "${UNITS[@]}"; do
  place "$HERE/$unit" "$UNIT_DIR/$unit"
done
place "$HERE/needrestart.conf" "$NEEDRESTART_DIR/marketlake.conf"

# 2. Retire a com.marketlake unit the render no longer names. A timer's stamp goes
# with it, or a reinstall of that timer would fire a replay at once.
for path in "$UNIT_DIR"/com.marketlake.*.service "$UNIT_DIR"/com.marketlake.*.timer; do
  if [[ ! -f "$path" ]]; then continue; fi
  unit="${path##*/}"
  if rendered "$unit"; then continue; fi
  echo "+ systemctl stop $unit"
  systemctl stop "$unit"
  echo "+ systemctl disable $unit"
  systemctl disable "$unit"
  if [[ "$unit" == *.timer ]]; then
    echo "+ rm -f $STAMP_DIR/stamp-$unit"
    rm -f "$STAMP_DIR/stamp-$unit"
  fi
  echo "+ rm -f $path"
  rm -f "$path"
done

# 3. Reload, so systemd reads what step 1 and step 2 changed. It restarts nothing.
echo "+ systemctl daemon-reload"
systemctl daemon-reload

# 4. Enable and start the residents and the timers. A unit already running is left
# running, and a timer-run service is started only by its timer.
for unit in "${ENABLE[@]}"; do
  echo "+ systemctl enable --now $unit"
  systemctl enable --now "$unit"
done

# 5. Read each resident back. enable --now exits 0 whatever the start did, so this
# is the only place a resident that cannot start shows. It never fails the install.
READBACK=ActiveState,SubState,NRestarts,Result,ExecMainStatus
for unit in "${RESIDENTS[@]}"; do
  echo "+ systemctl show --property=$READBACK $unit"
  systemctl show --property="$READBACK" "$unit" | sed "s/^/  /" || true
done
