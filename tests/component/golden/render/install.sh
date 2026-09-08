#!/bin/bash
# Marketlake control plane: the first install, steps 1 to 5.
#
# Written by `python -m lake.control_plane render`, which never runs it. Run it
# yourself, as the owner. It calls sudo for the privileged steps and will prompt.
#
# It stops at the first failure, so a visudo that rejects the drop-in never
# reaches the install that would place it. Every command is echoed before it runs.
# The last command reads back whether the daemon came up.
#
# Step 6, the standing Friday one-shot, is not here. It is not part of the first
# install. Run it from the install text.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 1. Install the five LaunchDaemons, root-owned as launchd requires.
echo '+ sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.daemon.plist" /Library/LaunchDaemons/'
sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.daemon.plist" /Library/LaunchDaemons/
echo '+ sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.dashboard.plist" /Library/LaunchDaemons/'
sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.dashboard.plist" /Library/LaunchDaemons/
echo '+ sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.self-check.plist" /Library/LaunchDaemons/'
sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.self-check.plist" /Library/LaunchDaemons/
echo '+ sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.calendar-probe.plist" /Library/LaunchDaemons/'
sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.calendar-probe.plist" /Library/LaunchDaemons/
echo '+ sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.sunday.plist" /Library/LaunchDaemons/'
sudo install -o root -g wheel -m 644 "$HERE/com.marketlake.sunday.plist" /Library/LaunchDaemons/
# 2. Install the sudoers drop-in after visudo validates it.
echo '+ sudo visudo -cf "$HERE/marketlake.sudoers"'
sudo visudo -cf "$HERE/marketlake.sudoers"
echo '+ sudo install -o root -g wheel -m 440 "$HERE/marketlake.sudoers" /etc/sudoers.d/marketlake'
sudo install -o root -g wheel -m 440 "$HERE/marketlake.sudoers" /etc/sudoers.d/marketlake
# visudo checks the syntax only. This prints the two rules as sudo parsed them,
# which is what shows the one-shot's regular expression survived as one.
echo '+ sudo -l | grep pmset'
sudo -l | grep pmset
# 3. Set the weekday firmware wake, then read it back.
echo '+ sudo pmset repeat wakeorpoweron MTWRF 08:25:00'
sudo pmset repeat wakeorpoweron MTWRF 08:25:00
echo '+ pmset -g sched'
pmset -g sched
# 4. Keep the token and the config secrets out of Time Machine. As the owner,
# never under sudo. The whole directory goes, so an editor that saves by rename
# cannot drop the exclusion, and config.yaml's four secrets are covered too.
echo '+ tmutil addexclusion /Users/someone/.config/marketlake'
tmutil addexclusion /Users/someone/.config/marketlake
echo '+ tmutil isexcluded /Users/someone/.config/marketlake'
tmutil isexcluded /Users/someone/.config/marketlake
# 5. Load the jobs into the system domain, then confirm the daemon is running.
echo '+ sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.daemon.plist'
sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.daemon.plist
echo '+ sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.dashboard.plist'
sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.dashboard.plist
echo '+ sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.self-check.plist'
sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.self-check.plist
echo '+ sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.calendar-probe.plist'
sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.calendar-probe.plist
echo '+ sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.sunday.plist'
sudo launchctl bootstrap system /Library/LaunchDaemons/com.marketlake.sunday.plist
echo '+ launchctl print system/com.marketlake.daemon'
launchctl print system/com.marketlake.daemon
