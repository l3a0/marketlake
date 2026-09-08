#!/bin/bash
# Marketlake control plane: re-install after a re-render.
#
# Written by `python -m lake.control_plane render`, which never runs it. Run it
# yourself, as the owner. It calls sudo for the privileged steps and will prompt.
#
# Usage. It installs the files sitting beside it, so it runs from anywhere:
#
#     ./reinstall.sh
#
# It boots each label out before re-installing it. Overwriting a plist alone does
# nothing, because launchd keeps the definition from the bootstrap that loaded it.
# A bootout of a label that is not loaded is skipped rather than treated as a
# failure, so this converges whether or not the jobs are currently running.
#
# It re-installs the launchd jobs and nothing else. The sudoers drop-in, the
# firmware wake and the Time Machine exclusion are steps 2, 3 and 4 of the
# install, and this does not repeat them. They usually survive a re-render, but
# not always. The owner, the home and both wake constants all feed them. A wake
# re-tune is the sharp case: it rewrites the sudoers rule while leaving every
# plist identical, so this script would reinstall nothing that changed. After a
# re-render that moved any of those, compare and re-run steps 2 to 4 by hand:
#
#     sudo diff /etc/sudoers.d/marketlake "$HERE/marketlake.sudoers"
#
# The `capture` check stays armed across this, because a check leaves its `new`
# state once and never returns. So the daemon going down here pages after the
# grace, the same as any other outage. That is correct rather than a nuisance, and
# it is the reason to keep this run short.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# com.marketlake.daemon
if launchctl print system/com.marketlake.daemon >/dev/null 2>&1; then
  echo '+ sudo launchctl bootout system/com.marketlake.daemon'
  sudo launchctl bootout system/com.marketlake.daemon
else
  echo '  system/com.marketlake.daemon is not loaded, nothing to boot out'
fi
echo '+ sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.daemon.plist" /Library/LaunchDaemons/'
sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.daemon.plist" /Library/LaunchDaemons/
echo '+ sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.daemon.plist'
sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.daemon.plist
# com.marketlake.dashboard
if launchctl print system/com.marketlake.dashboard >/dev/null 2>&1; then
  echo '+ sudo launchctl bootout system/com.marketlake.dashboard'
  sudo launchctl bootout system/com.marketlake.dashboard
else
  echo '  system/com.marketlake.dashboard is not loaded, nothing to boot out'
fi
echo '+ sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.dashboard.plist" /Library/LaunchDaemons/'
sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.dashboard.plist" /Library/LaunchDaemons/
echo '+ sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.dashboard.plist'
sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.dashboard.plist
# com.marketlake.self-check
if launchctl print system/com.marketlake.self-check >/dev/null 2>&1; then
  echo '+ sudo launchctl bootout system/com.marketlake.self-check'
  sudo launchctl bootout system/com.marketlake.self-check
else
  echo '  system/com.marketlake.self-check is not loaded, nothing to boot out'
fi
echo '+ sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.self-check.plist" /Library/LaunchDaemons/'
sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.self-check.plist" /Library/LaunchDaemons/
echo '+ sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.self-check.plist'
sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.self-check.plist
# com.marketlake.calendar-probe
if launchctl print system/com.marketlake.calendar-probe >/dev/null 2>&1; then
  echo '+ sudo launchctl bootout system/com.marketlake.calendar-probe'
  sudo launchctl bootout system/com.marketlake.calendar-probe
else
  echo '  system/com.marketlake.calendar-probe is not loaded, nothing to boot out'
fi
echo '+ sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.calendar-probe.plist" /Library/LaunchDaemons/'
sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.calendar-probe.plist" /Library/LaunchDaemons/
echo '+ sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.calendar-probe.plist'
sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.calendar-probe.plist
# com.marketlake.sunday
if launchctl print system/com.marketlake.sunday >/dev/null 2>&1; then
  echo '+ sudo launchctl bootout system/com.marketlake.sunday'
  sudo launchctl bootout system/com.marketlake.sunday
else
  echo '  system/com.marketlake.sunday is not loaded, nothing to boot out'
fi
echo '+ sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.sunday.plist" /Library/LaunchDaemons/'
sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.sunday.plist" /Library/LaunchDaemons/
echo '+ sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.sunday.plist'
sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.sunday.plist

# Read back whether the daemon came up under the new definition.
echo '+ launchctl print system/com.marketlake.daemon'
launchctl print system/com.marketlake.daemon
