#!/bin/bash
# Marketlake control plane: reinstall, which is uninstall then install.
#
# Written by `python -m lake.control_plane render`, which never runs it. Run it
# yourself, as the owner. Both halves call sudo and will prompt.
#
# Usage:
#
#     ./reinstall.sh
#
# It has no steps of its own. Everything it does comes from the two scripts beside
# it, so a reinstall cannot drift from what an install and an uninstall mean. In
# particular it re-runs the sudoers drop-in and the firmware wake, which the
# earlier in-place plist swap left to a hand-run `sudo diff`.
#
# It stops at the first failure. An uninstall that cannot finish leaves the install
# half unrun rather than layering a new install over a broken one.
#
# Read uninstall.sh's header first. Its step 2 cancels the whole repeating power
# pair, so a repeating sleep or shutdown set outside marketlake is cancelled here
# and not re-set by the install half.
#
# The lake, the config directory and that directory's Time Machine exclusion all
# survive, because the uninstall half does not touch them.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo '== uninstalling'
"$HERE/uninstall.sh"

echo '== installing'
"$HERE/install.sh"
