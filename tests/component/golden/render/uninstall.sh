#!/bin/bash
# Marketlake control plane: take the install back off, in reverse order.
#
# Written by `python -m lake.control_plane render`, which never runs it. Run it
# yourself, as the owner. It calls sudo for the privileged steps and will prompt.
#
# Usage:
#
#     ./uninstall.sh
#
# It boots out the five launchd jobs, cancels the weekday firmware wake, removes
# the sudoers drop-in, and deletes the five plists. That is install steps 5, 3, 2
# and 1, undone in that order.
#
# Three things it leaves:
#
#   - The lake. Deleting captured data is not part of undoing an install.
#   - The config directory AND its Time Machine exclusion. The token,
#     config.yaml and tickers.yaml all survive, so the protection on them
#     survives too. Lifting the exclusion would put the token and config.yaml's
#     secrets on the next hourly backup.
#   - The Sunday one-shot wake. Cancelling one event by name needs the exact
#     date it was set for, which nothing here knows without parsing `pmset -g
#     sched`. It fires once and is then gone.
#
# READ THIS BEFORE STEP 2. macOS holds one PAIR of repeating power events, a
# power-on and a power-off, and `pmset repeat cancel` clears the pair. No command
# cancels half of it. So a repeating sleep or shutdown you set elsewhere goes with
# the 08:25 wake. Step 2 prints the schedule before and after for that reason.
# Anything in the first print that is not the marketlake wake is yours to re-set.
# That holds on the reinstall path too: install.sh re-sets the 08:25 wake and
# nothing else, so it does not put back what step 2 took from you.
#
# Reinstalling after a re-render is this script and then the install, in one go:
#
#     ./uninstall.sh && ./install.sh
#
# The `&&` is load-bearing. If this half cannot finish, the install half must not
# run, rather than layering a new install over a broken one. A `;` would run it.
# There is no third script. A reinstall is these two, in that order, and nothing
# else, so it cannot drift from what an install and an uninstall mean.
#
# Four dead-man checks go silent when these jobs stop: capture, pre-open,
# calendar-probe and sunday. Each pages once its own deadline passes, which for
# capture is inside the weekday capture window and for sunday is Sunday 23:30.
# Pause all four from healthchecks first if the machine is meant to stay
# uninstalled. A check that has been pinged once does not go back to `new` on its
# own, so simply stopping the jobs is not enough to keep them quiet.
set -euo pipefail

# 1. Boot the five jobs out. This undoes install step 5, and it leads so that
# no plist below is deleted while launchd still holds its definition.
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
# 2. Cancel the weekday firmware wake. This undoes install step 3. The read-back
# runs first as well as last, because the cancel takes the whole repeating pair
# and the first print is the only record of what else was in it.
echo '+ pmset -g sched'
pmset -g sched
echo '+ sudo pmset repeat cancel'
sudo pmset repeat cancel
echo '+ pmset -g sched'
pmset -g sched
# 3. Remove the sudoers drop-in. This undoes install step 2. It grants two
# pmset writes and nothing this script runs, so nothing above depended on it.
echo '+ sudo rm -f /etc/sudoers.d/marketlake'
sudo rm -f /etc/sudoers.d/marketlake
# 4. Delete the five plists. This undoes install step 1, the first thing the
# install placed and so the last thing to come off.
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
# 5. Confirm the daemon is gone. Here the read-back is expected to fail, and
# that failure is the success condition, so it is handled rather than fatal.
echo '+ launchctl print system/com.marketlake.daemon'
if launchctl print system/com.marketlake.daemon >/dev/null 2>&1; then
  echo '  WARNING: com.marketlake.daemon is still loaded'
  exit 1
else
  echo '  com.marketlake.daemon is gone'
fi
