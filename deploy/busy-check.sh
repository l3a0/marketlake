# Marketlake: whether a scheduled job or a compaction is running on the hosted VM.
#
# Sourced, never run. deploy/vm-deploy.sh sources it once at load, before it moves the
# checkout, so the merge cannot change the check partway through a deploy that calls it
# again afterwards. deploy/vm-stop.sh sources it before it powers the VM off. Both need
# the same answer to the same question, so the check lives in one file (marketlake #868).
#
# The sourcing script sets ROOT, the test prefix for system paths, which is empty on a real
# host. This file sets the two resident service names and the variables the functions
# write, and runs nothing else.
#
# Written for bash 3.2 as well as a current bash, because the test suite runs it on a Mac.

DAEMON=com.marketlake.daemon.service
DASHBOARD=com.marketlake.dashboard.service

# Reads three of a unit's properties. systemctl prints them in its own order, so each is
# matched by name.
P_ACTIVE=""
P_PID=""
P_CGROUP=""
props() {
  local out line
  P_ACTIVE=""
  P_PID=""
  P_CGROUP=""
  out="$(systemctl show -p ActiveState,MainPID,ControlGroup "$1")" || return 1
  while IFS= read -r line || [[ -n "$line" ]]; do
    case "$line" in
      ActiveState=*) P_ACTIVE="${line#*=}" ;;
      MainPID=*) P_PID="${line#*=}" ;;
      ControlGroup=*) P_CGROUP="${line#*=}" ;;
    esac
  done <<< "$out"
}

# Returns 0 when nothing is busy, 1 when something is, and 2 when it cannot tell, with
# BUSY_REASON saying which. It lists services only, because a waiting timer reads active.
# It reads the daemon's cgroup rather than TasksCurrent, which counts the daemon's own
# threads. A daemon that is not active, or has no MainPID, never counts as busy.
BUSY_REASON=""
busy_check() {
  local units unit active pid procs file
  BUSY_REASON=""
  if ! units="$(systemctl list-units --type=service --all --no-legend --plain 'com.marketlake.*')"; then
    BUSY_REASON="systemctl list-units failed"
    return 2
  fi
  while read -r unit _ active _ || [[ -n "${unit:-}" ]]; do
    case "${unit:-}" in
      ""|"$DAEMON"|"$DASHBOARD") continue ;;
    esac
    case "${active:-}" in
      inactive|failed) ;;
      *)
        BUSY_REASON="$unit is ${active:-in no state}"
        return 1 ;;
    esac
  done <<< "$units"
  if ! props "$DAEMON"; then
    BUSY_REASON="systemctl show failed for $DAEMON"
    return 2
  fi
  if [[ "$P_ACTIVE" != active || -z "$P_PID" || "$P_PID" == 0 ]]; then
    return 0
  fi
  if [[ -z "$P_CGROUP" ]]; then
    BUSY_REASON="$DAEMON has no control group"
    return 2
  fi
  file="$ROOT/sys/fs/cgroup$P_CGROUP/cgroup.procs"
  if ! procs="$(cat -- "$file")"; then
    BUSY_REASON="could not read the daemon's cgroup.procs"
    return 2
  fi
  while read -r pid || [[ -n "${pid:-}" ]]; do
    if [[ -n "${pid:-}" && "$pid" != "$P_PID" ]]; then
      BUSY_REASON="the daemon's cgroup holds process $pid beside its main process $P_PID, such as a compaction"
      return 1
    fi
  done <<< "$procs"
  return 0
}
