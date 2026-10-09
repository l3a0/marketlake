#!/bin/bash
# Marketlake control plane on a systemd host: take the units back off.
#
# Written by `python -m lake.control_plane render --init systemd`, which never runs
# it. Run it as root:
#
#     sudo ./uninstall.sh
#
# It disables and stops each unit that is installed, deletes the persistent timers'
# stamps, removes the ten units and the needrestart drop-in, and reloads systemd.
# It acts only on units whose files are present, so it finishes on a partial
# install too. It leaves the lake and the config directory.
#
# If this host is the primary, seven dead-man checks go silent when these units stop:
# capture, pre-open, sunday, compaction, calendar-probe, eod-sweep and evening-upload.
# Pause them in healthchecks first if the host is meant to stay uninstalled. A
# shadow host feeds none of them, so uninstalling one silences nothing.
set -euo pipefail

if [[ "$(id -u)" != 0 ]]; then
  echo "uninstall.sh: run this as root, for example with sudo" >&2
  exit 2
fi
ROOT="${MARKETLAKE_INSTALL_ROOT:-}"
if [[ -n "$ROOT" && "${MARKETLAKE_INSTALL_TEST:-}" != 1 ]]; then
  echo "uninstall.sh: MARKETLAKE_INSTALL_ROOT is $ROOT, which only a test may set." >&2
  echo "uninstall.sh: Unset it to act on the real system paths." >&2
  exit 2
fi
echo "uninstall.sh: install root ${ROOT:-/}"
UNIT_DIR="$ROOT/etc/systemd/system"
NEEDRESTART_DIR="$ROOT/etc/needrestart/conf.d"
STAMP_DIR="$ROOT/var/lib/systemd/timers"

UNITS=(com.marketlake.self-check.timer com.marketlake.calendar-probe.timer com.marketlake.sunday.timer com.marketlake.eod-sweep.timer com.marketlake.daemon.service com.marketlake.dashboard.service com.marketlake.self-check.service com.marketlake.calendar-probe.service com.marketlake.sunday.service com.marketlake.eod-sweep.service)
STAMPS=(stamp-com.marketlake.sunday.timer stamp-com.marketlake.eod-sweep.timer)

# 1. Disable and stop each unit whose file is present. systemd 255's disable fails
# on a missing unit file, so an absent one is skipped rather than fatal.
for unit in ${UNITS[@]+"${UNITS[@]}"}; do
  if [[ -f "$UNIT_DIR/$unit" ]]; then
    echo "+ systemctl disable --now $unit"
    systemctl disable --now "$unit"
  else
    echo "  $unit is not installed, nothing to disable"
  fi
done

# 2. Delete the persistent timers' stamps, so a reinstall does not replay them.
for stamp in ${STAMPS[@]+"${STAMPS[@]}"}; do
  echo "+ rm -f $STAMP_DIR/$stamp"
  rm -f "$STAMP_DIR/$stamp"
done

# 3. Remove the units and the drop-in.
for unit in ${UNITS[@]+"${UNITS[@]}"}; do
  echo "+ rm -f $UNIT_DIR/$unit"
  rm -f "$UNIT_DIR/$unit"
done
echo "+ rm -f $NEEDRESTART_DIR/marketlake.conf"
rm -f "$NEEDRESTART_DIR/marketlake.conf"

# 4. Reload, so systemd forgets the units.
echo "+ systemctl daemon-reload"
systemctl daemon-reload
echo "uninstall.sh: done. On a primary host the seven dead-man checks now go silent and page."
