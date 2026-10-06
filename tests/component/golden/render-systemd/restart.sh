#!/bin/bash
# Marketlake control plane on a systemd host: restart a resident unit so it picks
# up new code. Written by `python -m lake.control_plane render --init systemd`.
#
# Usage, as root:
#
#     sudo ./restart.sh            # dashboard only, the default
#     sudo ./restart.sh daemon     # daemon only
#     sudo ./restart.sh dashboard  # dashboard only
#     sudo ./restart.sh all        # every resident unit
#
# Only the residents go stale. Each holds the Python it imported at start, and the
# venv is an editable install of the checkout, so a new commit reaches a new
# process only. The timer jobs start fresh on every run.
#
# Restarting is not free. The dashboard drops its open connections and the daemon
# loses its in-flight cycle, so a bare invocation restarts the dashboard alone. A
# role change in config.yaml reaches the daemon only through the daemon argument,
# because the daemon builds its senders once at start.
#
# It exits 0 when every named unit restarted and stayed up, 1 when a unit is not
# loaded, needs the install first, or will not stay up, and 2 on usage.
set -euo pipefail

PROJECT_DIR=/home/someone/marketlake
OWNER=someone
SETTLE_SECONDS=3

case "${1:-dashboard}" in
  daemon) UNITS=(com.marketlake.daemon.service) ;;
  dashboard) UNITS=(com.marketlake.dashboard.service) ;;
  all) UNITS=(com.marketlake.daemon.service com.marketlake.dashboard.service) ;;
  *)
    echo 'usage: ./restart.sh [daemon|dashboard|all]' >&2
    exit 2 ;;
esac
if [[ "$(id -u)" != 0 ]]; then
  echo "restart.sh: run this as root, for example with sudo" >&2
  exit 2
fi

prop() {
  systemctl show --property="$2" --value "$1"
}

# MainPID reads 0 while no process runs, which this script treats as no pid.
pid_of() {
  local pid
  pid="$(prop "$1" MainPID || true)"
  if [[ "$pid" == "0" ]]; then pid=""; fi
  printf "%s" "$pid"
}

# git refuses a checkout another account owns, so it runs as the owner.
as_owner() {
  sudo -u "$OWNER" -H "$@"
}

# 1. What the restart will pick up. The services import from this tree.
echo '+ working tree at /home/someone/marketlake'
if as_owner git -C "$PROJECT_DIR" rev-parse --git-dir >/dev/null 2>&1; then
  # An unborn HEAD makes --git-dir succeed and --abbrev-ref fail, and an
  # unguarded substitution would end the run here under `set -e`.
  branch="$(as_owner git -C "$PROJECT_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
  : "${branch:=unknown}"
  echo "  branch: $branch"
  if [[ -n "$(as_owner git -C "$PROJECT_DIR" status --porcelain || true)" ]]; then
    echo '  WARNING: uncommitted changes, so the restart picks those up too'
  fi
  if [[ "$branch" != "main" ]]; then
    echo '  WARNING: not on main, so the restart runs branch code'
  fi
else
  echo '  not a git checkout, so no branch to report'
fi

# 2. Restart each named unit, and prove it came back and stayed.
for unit in "${UNITS[@]}"; do
  if [[ "$(prop "$unit" LoadState)" != "loaded" ]]; then
    echo "  $unit is not loaded. Run the install first." >&2
    exit 1
  fi
  # A unit file changed since the last daemon-reload restarts under its old
  # definition, so the install, which reloads, has to run first.
  if [[ "$(prop "$unit" NeedDaemonReload)" == "yes" ]]; then
    echo "  $unit changed on disk since systemd last read it, so a restart would run the old definition. Run the install first." >&2
    exit 1
  fi
  before="$(pid_of "$unit")"
  if [[ -z "$before" ]]; then
    echo "  $unit is loaded but not running, so this starts it"
  else
    echo "  $unit is pid $before"
  fi
  echo "+ systemctl restart $unit"
  if ! systemctl restart "$unit"; then
    echo "  WARNING: $unit will not stay up. systemctl restart failed." >&2
    echo "  Check journalctl -u $unit" >&2
    exit 1
  fi
  after=""
  for _ in 1 2 3 4 5; do
    after="$(pid_of "$unit")"
    if [[ -n "$after" && "$after" != "$before" ]]; then
      break
    fi
    sleep 1
  done
  if [[ -z "$after" ]]; then
    echo "  WARNING: $unit has no pid after the restart" >&2
    echo "  Check journalctl -u $unit" >&2
    exit 1
  fi
  if [[ "$after" == "$before" ]]; then
    echo "  WARNING: $unit is still pid $before, so it did not restart" >&2
    exit 1
  fi
  # A new pid is not yet a working service. A resident that dies on import gets a
  # fresh pid too, so the new one has to still be there a moment later.
  sleep "$SETTLE_SECONDS"
  settled="$(pid_of "$unit")"
  if [[ "$settled" != "$after" ]]; then
    echo "  WARNING: $unit will not stay up. pid went $before -> $after -> ${settled:-none}." >&2
    echo "  Check journalctl -u $unit" >&2
    exit 1
  fi
  echo "  $unit restarted: pid ${before:-none} -> $after, still up after ${SETTLE_SECONDS}s"
done
