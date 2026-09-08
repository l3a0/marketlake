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
# particular it re-runs the sudoers drop-in, the firmware wake and the Time Machine
# exclusion, which an in-place plist swap would have skipped.
#
# It stops at the first failure. An uninstall that cannot finish leaves the install
# half unrun rather than layering a new install over a broken one.
#
# The lake and the config directory survive, because the uninstall half does not
# touch them.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo '== uninstalling'
"$HERE/uninstall.sh"

echo '== installing'
"$HERE/install.sh"
