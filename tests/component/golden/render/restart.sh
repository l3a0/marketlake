#!/bin/bash
# Marketlake control plane: restart a resident job so it picks up new code.
#
# Written by `python -m lake.control_plane render`, which never runs it. Run it
# yourself, as the owner. The restart needs root and will prompt.
#
# Usage:
#
#     ./restart.sh            # the dashboard, the usual case
#     ./restart.sh daemon     # just daemon
#     ./restart.sh dashboard  # just dashboard
#     ./restart.sh all        # every resident job
#
# Only these jobs can go stale, and the reason is their shape. They are resident:
# launchd starts each once and KeepAlive relaunches it if it exits, so each holds
# the Python it imported at start. Editing the working tree does not reach a
# process already running. The other three jobs exec fresh on every fire, so they
# always run current code and never need this.
#
# `launchctl kickstart -k` restarts the process under the definition launchd
# already holds. That is the right tool when the code changed and the plist did
# not. It is the WRONG tool after a re-render that changed a plist, because the
# old definition is what gets restarted. For a changed plist, reinstall instead:
#
#     ./uninstall.sh && ./install.sh
#
# A restart is not free. The dashboard drops its open connections, and the daemon
# loses the in-flight cycle and its caffeinate assertion until it is back. So the
# default is the dashboard on its own, and the daemon has to be named.
#
# The services import from the working tree, so what they pick up is that tree as
# it stands right now, branch and uncommitted edits included. Step 1 prints it.
set -euo pipefail

PROJECT_DIR=/Users/someone/marketlake
DOMAIN=system

case "${1:-dashboard}" in
  daemon) LABELS=(com.marketlake.daemon) ;;
  dashboard) LABELS=(com.marketlake.dashboard) ;;
  all) LABELS=(com.marketlake.daemon com.marketlake.dashboard) ;;
  *)
    echo 'usage: ./restart.sh [daemon|dashboard|all]' >&2
    exit 2 ;;
esac

# launchctl print needs no root, and it is the only place a job's pid is stated.
pid_of() {
  launchctl print "$DOMAIN/$1" 2>/dev/null |
    sed -n 's/^[[:space:]]*pid = \([0-9][0-9]*\).*/\1/p' | head -1 || true
}

# 1. What the restart will pick up. The services import from this tree, so this is
# the code they will be running afterwards, not whatever was current at boot.
echo '+ working tree at /Users/someone/marketlake'
if git -C "$PROJECT_DIR" rev-parse --git-dir >/dev/null 2>&1; then
  branch="$(git -C "$PROJECT_DIR" rev-parse --abbrev-ref HEAD)"
  echo "  branch: $branch"
  if [[ -n "$(git -C "$PROJECT_DIR" status --porcelain)" ]]; then
    echo '  WARNING: uncommitted changes, so the restart picks those up too'
  fi
  if [[ "$branch" != "main" ]]; then
    echo '  WARNING: not on main, so the restart runs branch code'
  fi
else
  echo '  not a git checkout, so no branch to report'
fi

# 2. Restart each named job, and prove it restarted. A pid that did not change is
# a kickstart that did nothing, which is the failure worth catching.
for label in "${LABELS[@]}"; do
  before="$(pid_of "$label")"
  if [[ -z "$before" ]]; then
    echo "  $label is not loaded. Run the install first." >&2
    exit 1
  fi
  # If the pid is already gone, ps fails, and under `set -e` an unguarded
  # command substitution would end the run here having printed nothing.
  running_since="$(ps -o lstart= -p "$before" 2>/dev/null | sed 's/^ *//' || true)"
  : "${running_since:=unknown}"
  echo "  $label is pid $before, running since $running_since"
  echo "+ sudo launchctl kickstart -k $DOMAIN/$label"
  sudo launchctl kickstart -k "$DOMAIN/$label"
  # KeepAlive relaunches within seconds rather than instantly, so give it a few.
  after=""
  for _ in 1 2 3 4 5; do
    after="$(pid_of "$label")"
    if [[ -n "$after" && "$after" != "$before" ]]; then
      break
    fi
    sleep 1
  done
  if [[ -z "$after" ]]; then
    echo "  WARNING: $label has no pid after the restart" >&2
    exit 1
  fi
  if [[ "$after" == "$before" ]]; then
    echo "  WARNING: $label is still pid $before, so it did not restart" >&2
    exit 1
  fi
  echo "  $label restarted: pid $before -> $after"
done
