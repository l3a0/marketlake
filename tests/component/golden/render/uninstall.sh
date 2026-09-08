#!/bin/bash
# Marketlake control plane: remove what the install placed.
#
# Written by `python -m lake.control_plane render`, which never runs it. Run it
# yourself, as the owner. It calls sudo for the privileged steps and will prompt.
#
# Usage:
#
#     ./uninstall.sh
#
# It removes the five launchd jobs and their plists, the sudoers drop-in, the
# weekday firmware wake, and the Time Machine exclusion. That is everything the
# install placed, taken off in reverse order.
#
# It does NOT touch the lake, and it does NOT touch the config directory. The
# token, config.yaml and tickers.yaml all stay. Only the exclusion on that
# directory is lifted, not the directory. Removing the token would make this a
# re-auth, and removing the lake would make it data loss.
#
# The Sunday one-shot wake is left in place. Cancelling it needs `pmset schedule
# cancelall`, which takes every scheduled event on the machine. It fires once and
# is then gone.
#
# The `capture` check stays armed, because a check leaves its `new` state once and
# never returns. It pages after the grace once the daemon stops. Pause it from
# healthchecks first if the machine is meant to stay uninstalled.
set -euo pipefail

# 1. Boot the jobs out, then delete their plists.
if launchctl print system/com.marketlake.daemon >/dev/null 2>&1; then
  echo '+ sudo launchctl bootout system/com.marketlake.daemon'
  sudo launchctl bootout system/com.marketlake.daemon
else
  echo '  system/com.marketlake.daemon is not loaded, nothing to boot out'
fi
if launchctl print system/com.marketlake.dashboard >/dev/null 2>&1; then
  echo '+ sudo launchctl bootout system/com.marketlake.dashboard'
  sudo launchctl bootout system/com.marketlake.dashboard
else
  echo '  system/com.marketlake.dashboard is not loaded, nothing to boot out'
fi
if launchctl print system/com.marketlake.self-check >/dev/null 2>&1; then
  echo '+ sudo launchctl bootout system/com.marketlake.self-check'
  sudo launchctl bootout system/com.marketlake.self-check
else
  echo '  system/com.marketlake.self-check is not loaded, nothing to boot out'
fi
if launchctl print system/com.marketlake.calendar-probe >/dev/null 2>&1; then
  echo '+ sudo launchctl bootout system/com.marketlake.calendar-probe'
  sudo launchctl bootout system/com.marketlake.calendar-probe
else
  echo '  system/com.marketlake.calendar-probe is not loaded, nothing to boot out'
fi
if launchctl print system/com.marketlake.sunday >/dev/null 2>&1; then
  echo '+ sudo launchctl bootout system/com.marketlake.sunday'
  sudo launchctl bootout system/com.marketlake.sunday
else
  echo '  system/com.marketlake.sunday is not loaded, nothing to boot out'
fi
echo '+ sudo rm -f /Library/LaunchDaemons/com.marketlake.daemon.plist'
sudo rm -f /Library/LaunchDaemons/com.marketlake.daemon.plist
echo '+ sudo rm -f /Library/LaunchDaemons/com.marketlake.dashboard.plist'
sudo rm -f /Library/LaunchDaemons/com.marketlake.dashboard.plist
echo '+ sudo rm -f /Library/LaunchDaemons/com.marketlake.self-check.plist'
sudo rm -f /Library/LaunchDaemons/com.marketlake.self-check.plist
echo '+ sudo rm -f /Library/LaunchDaemons/com.marketlake.calendar-probe.plist'
sudo rm -f /Library/LaunchDaemons/com.marketlake.calendar-probe.plist
echo '+ sudo rm -f /Library/LaunchDaemons/com.marketlake.sunday.plist'
sudo rm -f /Library/LaunchDaemons/com.marketlake.sunday.plist
# 2. Lift the Time Machine exclusion. As the owner, never under sudo. The
# directory and everything in it stays, including the token.
echo '+ tmutil removeexclusion /Users/someone/.config/marketlake'
tmutil removeexclusion /Users/someone/.config/marketlake
# 3. Cancel the weekday firmware wake. `pmset repeat` holds one alarm, so
# this cancels ours and nothing else.
echo '+ sudo pmset repeat cancel'
sudo pmset repeat cancel
echo '+ pmset -g sched'
pmset -g sched
# 4. Remove the sudoers drop-in last, because the steps above want sudo.
echo '+ sudo rm -f /etc/sudoers.d/marketlake'
sudo rm -f /etc/sudoers.d/marketlake
# 5. Confirm the daemon is gone. Here the read-back is expected to fail, and
# that failure is the success condition, so it is handled rather than fatal.
echo '+ launchctl print system/com.marketlake.daemon'
if launchctl print system/com.marketlake.daemon >/dev/null 2>&1; then
  echo '  WARNING: com.marketlake.daemon is still loaded'
  exit 1
else
  echo '  com.marketlake.daemon is gone'
fi
