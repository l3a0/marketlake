#!/bin/bash
# Marketlake: deploy a commit on main to the hosted VM, and restart the daemon only when that
# is safe, marketlake #676.
#
# Usage, as root, from any directory:
#
#     sudo deploy/vm-deploy.sh --sha <40 hex digits> [--not-after <epoch seconds>]
#
# The owner runs it by hand, and the CI half of #676 runs it through SSM Run Command after
# each merge. It prints exactly one line on stdout, the outcome, and exits 0 to 3. infra/README.md
# lists every line with what the owner does next. The progress goes to
# /var/lib/marketlake/deploy.log, mode 0600, because a line from the bootstrap can carry a
# config value and SSM keeps only the start of stdout.
#
# The script has two halves.
#
#   The wrapper, which SSM or a hand run starts, validates its arguments, refuses a
#   request whose --not-after has passed, creates deploy.log, and refuses while the unit
#   marketlake-deploy.service is active. It then starts this same script as that unit
#   with systemd-run --wait, passing a result file on /run, and prints the line the inner
#   run wrote there. The unit sits outside the wrapper's process group, so SSM's timeout
#   or a cancelled command, which kill that group, never stop a deploy between its merge
#   and its restart. systemd-run's own exit is ignored, because a unit stopped by SIGTERM
#   counts as a clean exit. A missing or malformed result prints "outcome unknown".
#
#   The inner run, started with --inner, refuses unless systemd set INVOCATION_ID. In
#   order, it:
#
#     1. takes /run/marketlake-deploy.lock without waiting, so a direct --inner call
#        cannot overlap a wrapped one, and reads the record of the last deploy,
#        /var/lib/marketlake/deployed, once;
#     2. refuses with exit 2 unless the daemon's ActiveState is active. A stopped daemon
#        was stopped on purpose, and the bootstrap's enable --now would start it;
#     3. refuses with exit 3 at the wrong moment: when python -m lake.deploy_window, run
#        as the owner from the current venv, says no, when a com.marketlake.* service
#        other than the two residents is in a state other than inactive or failed, or
#        when the daemon's cgroup holds a process besides its main one, such as a
#        compaction. It keeps the window's next_span_start for step 6;
#     4. fetches origin main as the owner, and refuses with exit 2 a sha not on
#        origin/main, a sha behind HEAD, a dirty tree or a branch other than main. Root
#        then fast-forwards the checkout as the owner, holding the install lock;
#     5. runs deploy/vm-bootstrap.sh from the new checkout, on every run, under a cap;
#     6. decides whether a restart is owed: when the record is absent or names another
#        sha, or when needrestart names a com.marketlake.* service still mapping a
#        replaced library. After a bootstrap exit 0 or 4 it waits up to 15 minutes for
#        the busy checks of step 3 to clear, then restarts the daemon, checks that its
#        MainPID holds for 120 seconds, and restarts the dashboard. After any other exit
#        it restarts nothing;
#     7. rolls back after a failed daemon restart, or after a failed bootstrap in a run
#        that moved HEAD, by resetting to the previous HEAD and running the bootstrap
#        again. After a failed daemon restart it restarts the daemon again;
#     8. removes the record before every daemon restart, and writes it after a restart
#        that held, following a bootstrap that exited 0. So the record names the running
#        sha or is absent, and absent owes a restart.
#
# Every progress line ends in || true, through say, because under set -e a failed write
# to a full disk would otherwise stop the run between the merge and the bootstrap. Each
# command's own output is captured and logged the same way, and every line goes through
# /usr/bin/printf rather than a builtin, for the reason out() gives. The inner run's
# outcome starts as "outcome unknown", and an EXIT trap writes whatever it holds to the
# result file, so a run killed partway reports itself unknown. TERM, INT and HUP exit 1
# through a trap, because bash 3.2 enters the EXIT trap after SIGTERM with $? at 0.
#
# The caps below sum to 200 minutes, a rollback included, and lake.deploy_window refuses
# a start less than 210 minutes before its next refused span, so even a deploy that rolls
# back ends before that span opens. A test checks the sum against the window's margin.
#
# Unlike the refusals of the other deploy/ scripts, every last line goes to stdout, the
# usage error included, because CI reads the host's outcome from there. CI prints that
# line in a public log, so it holds only this script's own words, shas and counts, never
# a config value, a host path or a line of a step's output. A reason names the failed
# step and points at deploy.log. Exit 0 comes only with a "deployed:" line for the
# requested sha, and that line only with exit 0, and the wrapper reads any other pairing
# as "outcome unknown".
#
# docs/design.md and issue #676 carry the reasoning for each step.
#
# MARKETLAKE_INSTALL_ROOT prefixes every system path this script reads or writes:
# bootstrap.conf, /run, /var/lib/marketlake and the daemon's cgroup under /sys/fs/cgroup.
# It is for tests only, and is refused unless MARKETLAKE_INSTALL_TEST=1 is set.
#
# Written for bash 3.2 as well as a current bash, because the test suite runs it on a Mac.
set -euo pipefail

# -- the caps, which sum to 200 minutes ------------------------------------------------

FETCH_SECONDS=300                # git fetch, once
INSTALL_LOCK_WAIT_SECONDS=600    # the install lock, for the merge and for a rollback's reset
BOOTSTRAP_SECONDS=4500           # each of two bootstraps, the deploy's and a rollback's
BOOTSTRAP_KILL_SECONDS=60        # timeout's wait between SIGTERM and SIGKILL
BUSY_WAIT_SECONDS=900            # the wait for the busy checks before the restart
RESTART_CHECK_SECONDS=180        # each of two restarts with their hold check
REST_SECONDS=120                 # the window check, needrestart and the rest

POLL_SECONDS=30                  # how often the wait reads the busy checks
HOLD_SECONDS=120                 # how long the daemon's MainPID must hold after a restart

# -- names -----------------------------------------------------------------------------

DEPLOY_UNIT=marketlake-deploy
DEPLOY_SERVICE="$DEPLOY_UNIT.service"
DAEMON=com.marketlake.daemon.service
DASHBOARD=com.marketlake.dashboard.service
UNKNOWN="outcome unknown: read deploy.log"
FAILED_STEP=", with a failed step in deploy.log"
NOT_RUN_BEFORE=", which has not run before"
SHA_RE='^[0-9a-f]{40}$'
EPOCH_RE='^[0-9]+$'

# Every variable the exit trap reads is set before the trap is.
ROOT="${MARKETLAKE_INSTALL_ROOT:-}"
INNER=0
SHA=""
NOT_AFTER=""
RESULT=""
OUTCOME_CODE=1
OUTCOME_LINE="$UNKNOWN"

# Every line this script writes goes through /usr/bin/printf rather than a builtin. bash
# 3.2 keeps a builtin's failed write in its stdout buffer and flushes it into whatever
# stdout is next, such as a command substitution or the result file, so one failed write
# would corrupt every later read. A separate process takes its failure with it.
out() {
  /usr/bin/printf '%s\n' "$@" 2>/dev/null || true
}

say() {
  out "vm-deploy: $*"
}

# Records the outcome and ends the run. The exit trap reports it.
finish() {
  OUTCOME_CODE="$1"
  OUTCOME_LINE="$2"
  exit 0
}

# The wrapper, and an inner run with no result file, print the outcome and exit with its
# code. An inner run with a result file writes it there and exits 0 on every path it
# decides, or 1 from a signal trap.
on_exit() {
  if [[ $INNER == 1 && -n "$RESULT" ]]; then
    say "outcome: $OUTCOME_CODE $OUTCOME_LINE"
    out "$OUTCOME_CODE $OUTCOME_LINE" > "$RESULT" 2>/dev/null || true
    return
  fi
  out "$OUTCOME_LINE"
  exit "$OUTCOME_CODE"
}
trap on_exit EXIT
trap 'OUTCOME_CODE=1; OUTCOME_LINE="$UNKNOWN"; exit 1' TERM INT HUP

# -- arguments -------------------------------------------------------------------------

USAGE="not deployed: usage: vm-deploy.sh --sha <40 hex digits> [--not-after <epoch seconds>]"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --sha|--not-after)
      if [[ $# -lt 2 ]]; then finish 2 "$USAGE"; fi
      if [[ "$1" == --sha ]]; then SHA="$2"; else NOT_AFTER="$2"; fi
      shift 2 ;;
    --inner)
      INNER=1
      shift ;;
    *)
      finish 2 "$USAGE" ;;
  esac
done
if [[ ! "$SHA" =~ $SHA_RE ]]; then
  finish 2 "not deployed: --sha must be 40 lowercase hex digits"
fi
if [[ -n "$NOT_AFTER" && ! "$NOT_AFTER" =~ $EPOCH_RE ]]; then
  finish 2 "not deployed: --not-after must be epoch seconds"
fi

# Only a result file the wrapper could have made is ever written, so a stray variable
# cannot point root's write somewhere else.
if [[ $INNER == 1 ]]; then
  case "${MARKETLAKE_DEPLOY_RESULT:-}" in
    "$ROOT/run/marketlake-deploy.result."*) RESULT="$MARKETLAKE_DEPLOY_RESULT" ;;
  esac
fi

if [[ -n "$ROOT" && "${MARKETLAKE_INSTALL_TEST:-}" != 1 ]]; then
  finish 2 "not deployed: MARKETLAKE_INSTALL_ROOT is set, which only a test may do"
fi
if [[ "$(id -u)" != 0 ]]; then
  finish 2 "not deployed: run this as root, for example with sudo"
fi

expired() {
  [[ -n "$NOT_AFTER" && "$(date +%s)" -gt "$NOT_AFTER" ]]
}

# -- bootstrap.conf, parsed as vm-bootstrap.sh parses it --------------------------------

CONF="$ROOT/etc/marketlake/bootstrap.conf"
if [[ ! -f "$CONF" ]]; then
  finish 2 "not deployed: /etc/marketlake/bootstrap.conf is missing"
fi
OWNER=""
VOLUME_ID=""
while IFS= read -r line || [[ -n "$line" ]]; do
  if [[ -z "$line" ]]; then
    continue
  fi
  key="${line%%=*}"
  value="${line#*=}"
  if [[ "$key" == "$line" ]]; then
    finish 2 "not deployed: bootstrap.conf holds a line that is not KEY=VALUE"
  fi
  case "$key" in
    OWNER)
      if [[ -n "$OWNER" ]]; then finish 2 "not deployed: bootstrap.conf sets OWNER twice"; fi
      OWNER="$value" ;;
    LAKE_VOLUME_ID)
      if [[ -n "$VOLUME_ID" ]]; then finish 2 "not deployed: bootstrap.conf sets LAKE_VOLUME_ID twice"; fi
      VOLUME_ID="$value" ;;
    *)
      finish 2 "not deployed: bootstrap.conf holds an unknown key" ;;
  esac
done < "$CONF"
ACCOUNT_RE='^[A-Za-z_][A-Za-z0-9_.-]*$'
if [[ ! "$OWNER" =~ $ACCOUNT_RE ]]; then
  finish 2 "not deployed: bootstrap.conf must set OWNER to an account name"
fi
if ! ENTRY="$(getent passwd "$OWNER")"; then
  finish 2 "not deployed: the OWNER in bootstrap.conf names no account on this host"
fi
if [[ "${ENTRY%%:*}" != "$OWNER" ]]; then
  finish 2 "not deployed: the OWNER in bootstrap.conf is a uid. Give the account name"
fi
OWNER_HOME="$(printf '%s\n' "$ENTRY" | cut -d: -f6)"
if [[ -z "$OWNER_HOME" ]]; then
  finish 2 "not deployed: the OWNER in bootstrap.conf has no home directory"
fi

# An exported CDPATH would make cd search other directories and print the one it chose
# into this substitution. Emptying it for the one cd keeps the path exact.
CHECKOUT="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null && pwd)"
SELF="$CHECKOUT/deploy/${BASH_SOURCE[0]##*/}"
PYTHON="$CHECKOUT/.venv/bin/python"
RESTART_SH="$OWNER_HOME/.local/state/marketlake/systemd/restart.sh"
STATE_DIR="$ROOT/var/lib/marketlake"
DEPLOY_LOG="$STATE_DIR/deploy.log"
RECORD_FILE="$STATE_DIR/deployed"
INSTALL_LOCK="$ROOT/run/marketlake-install.lock"
DEPLOY_LOCK="$ROOT/run/marketlake-deploy.lock"

# =====================================================================================
# The wrapper
# =====================================================================================

if [[ $INNER == 0 ]]; then
  # SSM's total timeout is the delivery timeout plus the document's, so a command to an
  # agent that was down could otherwise start hours after its CI job ended.
  if expired; then
    finish 3 "not deployed: the request expired"
  fi
  if ! { mkdir -p "$STATE_DIR" && chmod 0700 "$STATE_DIR"; }; then
    finish 1 "not deployed: could not create /var/lib/marketlake at mode 0700"
  fi
  if ! { (umask 077 && : >> "$DEPLOY_LOG") && chmod 0600 "$DEPLOY_LOG"; }; then
    finish 1 "not deployed: could not create deploy.log at mode 0600"
  fi
  if systemctl is-active --quiet "$DEPLOY_SERVICE"; then
    finish 3 "not deployed: another deploy is running"
  fi
  mkdir -p "$ROOT/run" || finish 1 "not deployed: could not create /run"
  # A wrapper killed with its process group leaves its result file behind. A boot empties
  # /run, and this keeps a VM that never reboots from collecting them.
  find "$ROOT/run" -maxdepth 1 -type f -name 'marketlake-deploy.result.*' -mmin +1440 \
    -exec rm -f -- {} + 2>/dev/null || true
  if ! WRAPPER_RESULT="$(mktemp "$ROOT/run/marketlake-deploy.result.XXXXXX")"; then
    finish 1 "not deployed: could not create a result file in /run"
  fi
  # Without -p StandardOutput a transient unit writes to the journal, which is not mode
  # 0600. Without --pipe, a write the agent stopped reading cannot kill the deploy with
  # SIGPIPE.
  ENVS=(--setenv="PATH=$PATH" --setenv="MARKETLAKE_DEPLOY_RESULT=$WRAPPER_RESULT")
  if [[ "${MARKETLAKE_INSTALL_TEST:-}" == 1 ]]; then
    ENVS+=(--setenv="MARKETLAKE_INSTALL_ROOT=$ROOT" --setenv="MARKETLAKE_INSTALL_TEST=1")
  fi
  INNER_ARGS=(--inner --sha "$SHA")
  if [[ -n "$NOT_AFTER" ]]; then
    INNER_ARGS+=(--not-after "$NOT_AFTER")
  fi
  systemd-run --unit="$DEPLOY_UNIT" --wait --collect --quiet \
    -p "StandardOutput=append:$DEPLOY_LOG" -p "StandardError=append:$DEPLOY_LOG" \
    "${ENVS[@]}" "$SELF" "${INNER_ARGS[@]}" >&2 || true

  CONTENT=""
  READ=0
  if [[ -f "$WRAPPER_RESULT" ]] && CONTENT="$(cat -- "$WRAPPER_RESULT")"; then
    READ=1
  fi
  rm -f -- "$WRAPPER_RESULT" || true
  if [[ $READ == 1 && -z "$CONTENT" ]]; then
    # The unit never wrote. Either another wrapper started it first, so this start was
    # refused, or the unit died before its trap ran.
    if systemctl is-active --quiet "$DEPLOY_SERVICE"; then
      finish 3 "not deployed: another deploy is running"
    fi
    finish 1 "$UNKNOWN"
  fi
  CODE="${CONTENT%% *}"
  LINE="${CONTENT#* }"
  if [[ $READ == 0 || "$CONTENT" != *" "* || "$CONTENT" == *$'\n'* || -z "$LINE" ]]; then
    finish 1 "$UNKNOWN"
  fi
  # 0 to 3 only. 127 means a missing script to CI, and 194 would make the SSM agent
  # reboot the instance.
  # Exit 0 comes only with a deployed line for the requested sha, and that line only with
  # exit 0, because CI pairs them and reads any other pairing as unknown.
  OK=0
  if [[ "$LINE" == "deployed: $SHA" || "$LINE" == "deployed: $SHA (already current)" ]]; then
    OK=1
  fi
  case "$CODE" in
    0) if [[ $OK == 1 ]]; then finish 0 "$LINE"; fi ;;
    1|2|3) if [[ $OK == 0 ]]; then finish "$CODE" "$LINE"; fi ;;
  esac
  finish 1 "$UNKNOWN"
fi

# =====================================================================================
# The inner run
# =====================================================================================

if [[ -z "${INVOCATION_ID:-}" ]]; then
  finish 2 "not deployed: --inner runs only as the unit the wrapper starts"
fi
if expired; then
  finish 3 "not deployed: the request expired"
fi

# -- 1. the deploy's own lock, and the record ------------------------------------------

mkdir -p "$ROOT/run" || finish 1 "not deployed: could not create /run"
exec 8>"$DEPLOY_LOCK" || finish 1 "not deployed: could not open the deploy lock"
if ! flock -n 8; then
  finish 3 "not deployed: another deploy is running"
fi
say "deploying $SHA, invocation $INVOCATION_ID, at $(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Read once and kept, because the restart below removes it. Anything but a sha reads as
# absent, which owes a restart.
RECORDED=""
if [[ -f "$RECORD_FILE" ]]; then
  read -r RECORDED < "$RECORD_FILE" || true
  if [[ ! "${RECORDED:-}" =~ $SHA_RE ]]; then
    RECORDED=""
  fi
fi
say "the last recorded deploy is ${RECORDED:-none}"

as_owner() {
  sudo -u "$OWNER" -H "$@"
}

git_owner() {
  as_owner git -C "$CHECKOUT" "$@"
}

# Runs a command with its output captured and then logged, so a write to a full disk or a
# broken pipe cannot fail the command itself.
logged() {
  local text rc=0
  text="$("$@" 2>&1)" || rc=$?
  if [[ -n "$text" ]]; then
    out "$text"
  fi
  return "$rc"
}

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

# -- 2. the daemon is running ----------------------------------------------------------

# This runs before the window, because a stopped daemon reads an empty ControlGroup, and
# the cgroup check would then read the root cgroup and answer exit 3 rather than 2.
if ! props "$DAEMON"; then
  finish 1 "not deployed: systemctl show failed for $DAEMON"
fi
if [[ "$P_ACTIVE" != active ]]; then
  finish 2 "not deployed: $DAEMON is ${P_ACTIVE:-in no state}, not active. infra/README.md gives the hand procedure for a daemon that is not running"
fi

# -- 3. the moment ---------------------------------------------------------------------

# The current venv runs this, as the owner, so no root-owned cache lands in the checkout.
# It is the deploy's only Python before the merge, and the code already on the VM.
WINDOW_RC=0
WINDOW_OUT="$(as_owner "$PYTHON" -m lake.deploy_window)" || WINDOW_RC=$?
out "$WINDOW_OUT"
WINDOW_LINE=""
NEXT_SPAN_START=""
LINES=0
while IFS= read -r line || [[ -n "$line" ]]; do
  LINES=$((LINES + 1))
  case "$LINES" in
    1) WINDOW_LINE="$line" ;;
    2) NEXT_SPAN_START="${line#next_span_start=}"
       if [[ "$NEXT_SPAN_START" == "$line" ]]; then NEXT_SPAN_START=""; fi ;;
  esac
done <<< "$WINDOW_OUT"
case "$WINDOW_RC" in
  0|3) ;;
  *) finish 1 "not deployed: python -m lake.deploy_window exited $WINDOW_RC" ;;
esac
if [[ $LINES != 2 || -z "$WINDOW_LINE" || ! "$NEXT_SPAN_START" =~ $EPOCH_RE ]]; then
  finish 1 "not deployed: python -m lake.deploy_window printed no next_span_start line"
fi
# The window's line reaches a public log, so only its own shape passes.
WINDOW_LINE_RE='^a deploy may start next at [A-Z][a-z]{2} [0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2} [A-Z]{3,4}, because [a-z0-9 ]+$'
if [[ $WINDOW_RC == 3 ]]; then
  if [[ ! "$WINDOW_LINE" =~ $WINDOW_LINE_RE ]]; then
    finish 3 "not deployed: the window refuses a deploy now, see deploy.log"
  fi
  finish 3 "not deployed: $WINDOW_LINE"
fi

BUSY_RC=0
busy_check || BUSY_RC=$?
case "$BUSY_RC" in
  0) ;;
  1) finish 3 "not deployed: $BUSY_REASON" ;;
  *) finish 1 "not deployed: $BUSY_REASON" ;;
esac

# -- 4. forward only -------------------------------------------------------------------

say "fetching origin main as $OWNER"
if ! logged as_owner timeout "$FETCH_SECONDS" git -C "$CHECKOUT" fetch origin main; then
  finish 1 "not deployed: git fetch origin main failed or ran past $FETCH_SECONDS seconds"
fi
BRANCH="$(git_owner rev-parse --abbrev-ref HEAD 2>/dev/null)" || BRANCH=""
if [[ "$BRANCH" != main ]]; then
  finish 2 "not deployed: the checkout is not on main"
fi
if ! DIRTY="$(git_owner status --porcelain)"; then
  finish 1 "not deployed: git status failed, see deploy.log"
fi
if [[ -n "$DIRTY" ]]; then
  finish 2 "not deployed: the checkout has uncommitted changes"
fi
if ! PREV="$(git_owner rev-parse HEAD)" || [[ ! "$PREV" =~ $SHA_RE ]]; then
  finish 1 "not deployed: could not read the checkout's HEAD"
fi
# --is-ancestor exits 1 for a sha that is not an ancestor and 128 for one never fetched,
# so any exit but 0 refuses.
if ! git_owner merge-base --is-ancestor "$SHA" origin/main >/dev/null 2>&1; then
  finish 2 "not deployed: $SHA is not on origin/main"
fi
# merge --ff-only alone would accept an older sha as "Already up to date".
if ! git_owner merge-base --is-ancestor "$PREV" "$SHA" >/dev/null 2>&1; then
  finish 2 "not deployed: $SHA is behind the checkout's HEAD, $PREV"
fi
# Root takes the install lock, because a boot empties /run and the owner cannot create the
# file there. flock releases it when the merge exits, before the bootstrap takes it.
say "fast-forwarding $CHECKOUT from $PREV to $SHA, holding $INSTALL_LOCK"
logged flock -w "$INSTALL_LOCK_WAIT_SECONDS" "$INSTALL_LOCK" \
  sudo -u "$OWNER" -H git -C "$CHECKOUT" merge --ff-only "$SHA" || true
HEAD_NOW="$(git_owner rev-parse HEAD)" || HEAD_NOW=""
if [[ "$HEAD_NOW" != "$SHA" ]]; then
  if [[ "$HEAD_NOW" == "$PREV" ]]; then
    finish 1 "not deployed: the merge of $SHA failed, or another run held the install lock for $INSTALL_LOCK_WAIT_SECONDS seconds"
  fi
  finish 1 "$UNKNOWN"
fi
MOVED=0
if [[ "$PREV" != "$SHA" ]]; then
  MOVED=1
fi

# -- 5. the bootstrap ------------------------------------------------------------------

# Its output goes to this run's stdout, which is deploy.log. A run that reaches the cap
# fails, with timeout's exit 124, or 137 after the SIGKILL.
bootstrap() {
  timeout -k "$BOOTSTRAP_KILL_SECONDS" "$BOOTSTRAP_SECONDS" "$CHECKOUT/deploy/vm-bootstrap.sh"
}

say "running deploy/vm-bootstrap.sh at $SHA"
BOOT_RC=0
bootstrap || BOOT_RC=$?
say "deploy/vm-bootstrap.sh exited $BOOT_RC"

# -- 6. whether a restart is owed ------------------------------------------------------

OWED=0
if [[ -z "$RECORDED" ]]; then
  say "a restart is owed, because no deploy is recorded"
  OWED=1
elif [[ "$RECORDED" != "$SHA" ]]; then
  say "a restart is owed, because the recorded deploy is $RECORDED"
  OWED=1
fi
# needrestart -b only lists. It never restarts anything.
if [[ $OWED == 0 ]]; then
  if command -v needrestart >/dev/null 2>&1; then
    NR_RC=0
    NR_OUT="$(needrestart -b -r l 2>&1)" || NR_RC=$?
    out "$NR_OUT"
    if [[ $NR_RC != 0 ]]; then
      say "a restart is owed, because needrestart exited $NR_RC"
      OWED=1
    else
      while IFS= read -r line || [[ -n "$line" ]]; do
        case "$line" in
          "NEEDRESTART-SVC: com.marketlake."*)
            say "a restart is owed, because needrestart lists ${line#NEEDRESTART-SVC: }"
            OWED=1 ;;
        esac
      done <<< "$NR_OUT"
    fi
  else
    say "needrestart is not installed, so no library check owes a restart"
  fi
fi

NOT_RESTARTED="not restarted: the tree is at $SHA, and the last recorded deploy is ${RECORDED:-none}"

# Waits for the busy checks to clear, polling, and gives up when the wait runs out or the
# next refused span is too close for a restart and its check. No Python runs here, since
# the venv now imports the new commit.
wait_until_clear() {
  local polls=$((BUSY_WAIT_SECONDS / POLL_SECONDS)) n=0 rc
  while :; do
    if [[ $(( $(date +%s) + RESTART_CHECK_SECONDS )) -ge $NEXT_SPAN_START ]]; then
      say "the next refused span starts within $RESTART_CHECK_SECONDS seconds"
      return 1
    fi
    rc=0
    busy_check || rc=$?
    if [[ $rc == 0 ]]; then
      return 0
    fi
    say "waiting, because $BUSY_REASON"
    n=$((n + 1))
    if [[ $n -gt $polls ]]; then
      say "still waiting after $BUSY_WAIT_SECONDS seconds"
      return 1
    fi
    sleep "$POLL_SECONDS"
  done
}

# Restarts the daemon and proves it held. restart.sh proves only three seconds, so this
# also waits HOLD_SECONDS and finds the same MainPID. RESTART_REASON says why it failed.
RESTART_REASON=""
restart_daemon() {
  local before rc=0
  RESTART_REASON=""
  if ! rm -f -- "$RECORD_FILE"; then
    RESTART_REASON="could not remove the record of the last deploy"
    return 1
  fi
  say "restarting the daemon"
  logged "$RESTART_SH" daemon || rc=$?
  if [[ $rc != 0 ]]; then
    RESTART_REASON="restart.sh daemon exited $rc"
    return 1
  fi
  if ! props "$DAEMON" || [[ "$P_ACTIVE" != active || -z "$P_PID" || "$P_PID" == 0 ]]; then
    RESTART_REASON="the daemon was not running after its restart"
    return 1
  fi
  before="$P_PID"
  sleep "$HOLD_SECONDS"
  if ! props "$DAEMON" || [[ "$P_ACTIVE" != active || "$P_PID" != "$before" ]]; then
    RESTART_REASON="the daemon did not stay up for $HOLD_SECONDS seconds after its restart"
    return 1
  fi
  say "the daemon held pid $before for $HOLD_SECONDS seconds"
  return 0
}

# Writes the record through a temporary file in the same directory, then a rename.
write_record() {
  local tmp
  tmp="$(mktemp "$STATE_DIR/deployed.XXXXXX")" || return 1
  if /usr/bin/printf '%s\n' "$1" > "$tmp" && mv -f -- "$tmp" "$RECORD_FILE"; then
    say "recorded $1"
    return 0
  fi
  rm -f -- "$tmp" || true
  return 1
}

# -- 7. the rollback -------------------------------------------------------------------

# Returns the checkout to the HEAD this run found, and runs the bootstrap again. That HEAD,
# not the recorded sha, because its bootstrap installed the units, config and roster in
# place now. With a restart, it restarts the daemon again and nothing else.
rollback() {
  local reason="$1" restart="$2" rb=0 head suffix=""
  say "rolling back to $PREV, because $reason"
  logged flock -w "$INSTALL_LOCK_WAIT_SECONDS" "$INSTALL_LOCK" \
    sudo -u "$OWNER" -H git -C "$CHECKOUT" reset --keep "$PREV" || true
  head="$(git_owner rev-parse HEAD)" || head=""
  if [[ "$head" != "$PREV" ]]; then
    finish 1 "rollback to $PREV failed: $reason, and the reset to it failed"
  fi
  bootstrap || rb=$?
  say "the rollback's deploy/vm-bootstrap.sh exited $rb"
  case "$rb" in
    0|4) ;;
    *) finish 1 "rollback to $PREV failed: $reason, and the rollback's bootstrap exited $rb" ;;
  esac
  if [[ -n "$RECORDED" && "$RECORDED" != "$PREV" ]]; then
    suffix="$NOT_RUN_BEFORE"
  fi
  if [[ $restart == 1 ]]; then
    if ! restart_daemon; then
      finish 1 "rollback to $PREV failed: $reason, and then $RESTART_REASON"
    fi
    if [[ $rb != 0 ]] || ! write_record "$PREV"; then
      suffix="$suffix$FAILED_STEP"
    fi
  elif [[ $rb != 0 ]]; then
    suffix="$suffix$FAILED_STEP"
  fi
  finish 1 "rolled back to $PREV: $reason$suffix"
}

# -- 6, continued, and 8. the restart and the record -----------------------------------

case "$BOOT_RC" in
  0|4) ;;
  *)
    # The daemon still runs the code it started with, so nothing restarts. A bootstrap
    # that failed on a moved tree is undone.
    REASON="the bootstrap of $SHA exited $BOOT_RC"
    if [[ $MOVED == 1 ]]; then
      rollback "$REASON" 0
    fi
    if [[ $OWED == 1 ]]; then
      finish 1 "$NOT_RESTARTED"
    fi
    finish 1 "not deployed: $REASON" ;;
esac

if [[ $OWED == 0 ]]; then
  if [[ $BOOT_RC == 0 ]]; then
    finish 0 "deployed: $SHA (already current)"
  fi
  finish 1 "deployed: $SHA$FAILED_STEP"
fi

# A wait that runs out leaves the restart owed. It never rolls back, because a rollback
# would change the tree while a busy service reads it, or inside the span.
if ! wait_until_clear; then
  finish 1 "$NOT_RESTARTED"
fi
if ! restart_daemon; then
  rollback "$RESTART_REASON" 1
fi
SUFFIX=""
if [[ $BOOT_RC != 0 ]] || ! write_record "$SHA"; then
  SUFFIX="$FAILED_STEP"
fi
# The dashboard captures nothing, so its failure rolls nothing back.
say "restarting the dashboard"
if ! logged "$RESTART_SH" dashboard; then
  SUFFIX="$SUFFIX, but the dashboard did not restart"
fi
if [[ -z "$SUFFIX" ]]; then
  finish 0 "deployed: $SHA"
fi
finish 1 "deployed: $SHA$SUFFIX"
