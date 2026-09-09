#!/bin/bash
# Marketlake control plane: restart a resident job so it picks up new code.
#
# Written by `python -m lake.control_plane render`, which never runs it. Run it
# yourself, as the owner. The restart needs root and will prompt.
#
# Usage:
#
#     ./restart.sh            # dashboard only, the default
#     ./restart.sh daemon     # daemon only
#     ./restart.sh dashboard  # dashboard only
#     ./restart.sh all        # every resident job
#
# Only these jobs can go stale, and the reason is their shape. They are resident:
# launchd starts each once and KeepAlive relaunches it if it exits, so each holds
# the Python it imported at start. The venv is an editable install pointing at an
# absolute src directory, so editing that tree changes what a NEW process imports
# and nothing about one already running. The other three jobs exec fresh on every
# fire, so they always run current code and never need this.
#
# `launchctl kickstart -k` runs the service immediately whatever its launch
# conditions say, killing the running instance first if there is one. That is the
# right tool when the code changed and the plist did not. It is the WRONG tool
# after a re-render that changed a plist, because the old definition is what gets
# run. For a changed plist, reinstall instead:
#
#     ./uninstall.sh && ./install.sh
#
# That reinstall does restart both residents on the way through, so it is not that
# this case had no tool. It is that the only tool was one that takes the whole
# control plane off and puts it back to achieve a process restart.
#
# Restarting is not free. The dashboard drops its open connections, and the daemon
# loses the in-flight cycle and its caffeinate assertion until it is back. So a
# bare invocation restarts the dashboard alone and the daemon has to be named.
#
# The services import from the working tree, so what they pick up is that tree as
# it stands right now, branch and uncommitted edits included. Step 1 prints it.
set -euo pipefail

PROJECT_DIR=/Users/someone/marketlake
LOG_DIR=/Users/someone/Library/Logs/marketlake
DOMAIN=system
SETTLE_SECONDS=3

case "${1:-dashboard}" in
  daemon) LABELS=(com.marketlake.daemon) ;;
  dashboard) LABELS=(com.marketlake.dashboard) ;;
  all) LABELS=(com.marketlake.daemon com.marketlake.dashboard) ;;
  *)
    echo 'usage: ./restart.sh [daemon|dashboard|all]' >&2
    exit 2 ;;
esac

# Two questions, not one. `launchctl print` exits 0 for any label in the domain
# and 113 for one that is not. The pid line appears only while a process is
# actually running, so a loaded job between processes prints no pid and reads
# exactly like one that was never installed. Neither needs root.
is_loaded() {
  launchctl print "$DOMAIN/$1" >/dev/null 2>&1
}

pid_of() {
  launchctl print "$DOMAIN/$1" 2>/dev/null |
    sed -n 's/^[[:space:]]*pid = \([0-9][0-9]*\).*/\1/p' | head -1 || true
}

# 1. What the restart will pick up. The services import from this tree, so this is
# the code they will be running afterwards, not whatever was current at boot.
echo '+ working tree at /Users/someone/marketlake'
if git -C "$PROJECT_DIR" rev-parse --git-dir >/dev/null 2>&1; then
  # An unborn HEAD makes --git-dir succeed and --abbrev-ref fail, and an
  # unguarded substitution would end the run here under `set -e`.
  branch="$(git -C "$PROJECT_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
  : "${branch:=unknown}"
  echo "  branch: $branch"
  if [[ -n "$(git -C "$PROJECT_DIR" status --porcelain || true)" ]]; then
    echo '  WARNING: uncommitted changes, so the restart picks those up too'
  fi
  if [[ "$branch" != "main" ]]; then
    echo '  WARNING: not on main, so the restart runs branch code'
  fi
else
  echo '  not a git checkout, so no branch to report'
fi

# 2. Restart each named job, and prove it came back and stayed. A pid that did not
# change is a kickstart that did nothing. A pid that keeps changing is a job dying
# on the new code, which is the failure a restart is most likely to cause.
for label in "${LABELS[@]}"; do
  if ! is_loaded "$label"; then
    echo "  $label is not in the $DOMAIN domain. Run the install first." >&2
    exit 1
  fi
  before="$(pid_of "$label")"
  if [[ -z "$before" ]]; then
    echo "  $label is loaded but not running, so this starts it"
  else
    # If the pid is already gone, ps fails, and under `set -e` an unguarded
    # command substitution would end the run here having printed nothing.
    since="$(ps -o lstart= -p "$before" 2>/dev/null | sed 's/^ *//' || true)"
    : "${since:=an unknown time}"
    echo "  $label is pid $before, running since $since"
  fi
  echo "+ sudo launchctl kickstart -k $DOMAIN/$label"
  sudo launchctl kickstart -k "$DOMAIN/$label"
  # KeepAlive relaunches within seconds rather than instantly, so poll rather
  # than read once.
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
    echo "  Check $LOG_DIR/$label.err.log" >&2
    exit 1
  fi
  if [[ "$after" == "$before" ]]; then
    echo "  WARNING: $label is still pid $before, so it did not restart" >&2
    exit 1
  fi
  # A new pid is not yet a working service. A resident that dies on import gets a
  # fresh pid too, so the new one has to still be there a moment later.
  sleep "$SETTLE_SECONDS"
  settled="$(pid_of "$label")"
  if [[ "$settled" != "$after" ]]; then
    echo "  WARNING: $label will not stay up. pid went $before -> $after -> ${settled:-none}." >&2
    echo "  That is a job crash-looping on the new code. Check $LOG_DIR/$label.err.log" >&2
    exit 1
  fi
  echo "  $label restarted: pid $before -> $after, still up after ${SETTLE_SECONDS}s"
done
