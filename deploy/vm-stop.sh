#!/bin/bash
# Marketlake: power the hosted VM off once the day's work is done, marketlake #868.
#
# marketlake-stop.service runs this as root every 10 minutes, from marketlake-stop.timer.
# A schedule in AWS starts the VM again before it is next needed (marketlake #867). A VM
# left running costs cents an hour, while a stop at the wrong moment can cost a captured
# minute for good, so every check below fails toward staying up.
#
# It powers the VM off only when all six checks pass, in this order, cheapest first and
# the locks last.
#
#   1. switch: /etc/marketlake/stop-when-idle exists. The owner creates it to turn the
#      stop on, and removes it to keep the VM up for longer work, such as a restore or a
#      step run over SSM. Nothing stops until it exists, so it is also the rollout gate.
#   2. uptime: the VM has been up at least an hour, read from /proc/uptime. A VM the owner
#      starts by hand for a deploy gets that hour before the stop considers it.
#   3. terminal: no line of `loginctl list-sessions --no-legend` has a TTY in its fifth
#      field, or `closing` in its sixth, for any user. That is systemd 255's column order.
#      An interactive SSH session has a TTY, and a command left running after logout
#      keeps its session closing. The dashboard tunnel, ssh -N -L, has no TTY and does not
#      hold the VM up.
#   4. busy: the busy check in deploy/busy-check.sh finds no com.marketlake.* service
#      running other than the two residents, and no compaction in the daemon's cgroup.
#   5. window: python -m lake.deploy_window, run as the owner, exits 0. Exit 3 refuses,
#      and any other exit, or output without its next_span_start line, is an error. The
#      busy check skips the always-on daemon, so this is the check that keeps the stop out
#      of capture hours.
#   6. deploy: marketlake-deploy.service is not running. Then it takes
#      /run/marketlake-deploy.lock and /run/marketlake-install.lock without waiting,
#      checks the deploy unit again, and holds both locks through the poweroff. Once the
#      poweroff is queued, systemd refuses to start a new deploy unit.
#
# A refusal prints one line on stdout naming its check, and exits 0, so the unit is not
# marked failed about 140 times a day. An error, a check that cannot tell, prints its
# reason on stderr and exits 1, so it stands apart from a routine refusal. A setup error,
# such as a run without root, exits 2. journalctl -u marketlake-stop reads the lines.
#
# When every check passes, it pings the vm-stop health check as the owner, then runs
# systemctl poweroff whatever the ping returned. deploy_window and the ping run as the
# owner, through sudo -u like deploy/vm-deploy.sh, with the owner read from bootstrap.conf.
# Run as root the ping would find no config, and either could leave root-owned files in
# the owner's checkout or outbox. sudo clears the environment, so a MARKETLAKE_CONFIG the
# unit sets reaches the ping as --config.
#
# MARKETLAKE_INSTALL_ROOT prefixes every system path this script reads or writes:
# the switch, /proc/uptime, bootstrap.conf, /run and the daemon's cgroup. It is for tests
# only, and is refused unless MARKETLAKE_INSTALL_TEST=1 is set.
#
# docs/design.md and issue #865 carry the reasoning for each check.
#
# Written for bash 3.2 as well as a current bash, because the test suite runs it on a Mac.
set -euo pipefail

UPTIME_SECONDS=3600
SWITCH=/etc/marketlake/stop-when-idle
UPTIME=/proc/uptime
DEPLOY_SERVICE=marketlake-deploy.service
DEPLOY_LOCK=/run/marketlake-deploy.lock
INSTALL_LOCK=/run/marketlake-install.lock
SLUG=vm-stop
# No leading zero, because bash reads one as octal.
NUMBER_RE='^(0|[1-9][0-9]*)$'
ROOT="${MARKETLAKE_INSTALL_ROOT:-}"

say() {
  printf '%s\n' "vm-stop: $*" || true
}

# The VM stays up because one check says so, which is routine.
refuse() {
  say "staying up, check $1: $2"
  exit 0
}

# A check could not tell, so the VM stays up and the unit is marked failed.
fail() {
  say "error, check $1: $2" >&2
  exit 1
}

# The script cannot run at all.
setup_error() {
  say "error: $*" >&2
  exit 2
}

if [[ $# -gt 0 ]]; then
  setup_error "usage: vm-stop.sh, with no arguments"
fi
if [[ -n "$ROOT" && "${MARKETLAKE_INSTALL_TEST:-}" != 1 ]]; then
  setup_error "MARKETLAKE_INSTALL_ROOT is set, which only a test may do"
fi
if [[ "$(id -u)" != 0 ]]; then
  setup_error "run this as root, as marketlake-stop.service does"
fi

# An exported CDPATH would make cd search other directories and print the one it chose
# into this substitution. Emptying it for the one cd keeps the path exact.
CHECKOUT="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null && pwd)"
PYTHON="$CHECKOUT/.venv/bin/python"

# -- 1. switch -------------------------------------------------------------------------

if [[ ! -e "$ROOT$SWITCH" ]]; then
  refuse "1 (switch)" "$SWITCH does not exist, so the stop is off"
fi

# -- 2. uptime -------------------------------------------------------------------------

UP=""
if [[ -r "$ROOT$UPTIME" ]]; then
  read -r UP _ < "$ROOT$UPTIME" || true
fi
UP="${UP%%.*}"
if [[ ! "$UP" =~ $NUMBER_RE ]]; then
  fail "2 (uptime)" "$UPTIME holds no uptime in seconds"
fi
if [[ "$UP" -lt "$UPTIME_SECONDS" ]]; then
  refuse "2 (uptime)" "the VM has been up $UP seconds, less than $UPTIME_SECONDS"
fi

# -- 3. terminal -----------------------------------------------------------------------

if ! SESSIONS="$(loginctl list-sessions --no-legend)"; then
  fail "3 (terminal)" "loginctl list-sessions failed"
fi
while read -r session _ user _ tty state _ || [[ -n "${session:-}" ]]; do
  if [[ -z "${session:-}" ]]; then
    continue
  fi
  if [[ -z "$state" ]]; then
    fail "3 (terminal)" "loginctl printed a session with fewer than six fields"
  fi
  if [[ "$tty" != - ]]; then
    refuse "3 (terminal)" "session $session of $user is at terminal $tty"
  fi
  if [[ "$state" == closing ]]; then
    refuse "3 (terminal)" "session $session of $user is closing, so a command it started may still run"
  fi
done <<< "$SESSIONS"

# -- 4. busy ---------------------------------------------------------------------------

# bash 3.2 ends the whole script when . cannot find its file, so the file is tested first.
if [[ ! -r "$CHECKOUT/deploy/busy-check.sh" ]] || ! . "$CHECKOUT/deploy/busy-check.sh"; then
  fail "4 (busy)" "could not read deploy/busy-check.sh"
fi
BUSY_RC=0
busy_check || BUSY_RC=$?
case "$BUSY_RC" in
  0) ;;
  1) refuse "4 (busy)" "$BUSY_REASON" ;;
  *) fail "4 (busy)" "$BUSY_REASON" ;;
esac

# -- 5. window -------------------------------------------------------------------------

# bootstrap.conf, parsed as deploy/vm-deploy.sh parses it. Only the owner is used here.
CONF="$ROOT/etc/marketlake/bootstrap.conf"
if [[ ! -f "$CONF" ]]; then
  fail "5 (window)" "/etc/marketlake/bootstrap.conf is missing"
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
    fail "5 (window)" "bootstrap.conf holds a line that is not KEY=VALUE"
  fi
  case "$key" in
    OWNER)
      if [[ -n "$OWNER" ]]; then fail "5 (window)" "bootstrap.conf sets OWNER twice"; fi
      OWNER="$value" ;;
    LAKE_VOLUME_ID)
      if [[ -n "$VOLUME_ID" ]]; then fail "5 (window)" "bootstrap.conf sets LAKE_VOLUME_ID twice"; fi
      VOLUME_ID="$value" ;;
    *)
      fail "5 (window)" "bootstrap.conf holds an unknown key" ;;
  esac
done < "$CONF"
ACCOUNT_RE='^[A-Za-z_][A-Za-z0-9_.-]*$'
if [[ ! "$OWNER" =~ $ACCOUNT_RE ]]; then
  fail "5 (window)" "bootstrap.conf must set OWNER to an account name"
fi
if ! ENTRY="$(getent passwd "$OWNER")"; then
  fail "5 (window)" "the OWNER in bootstrap.conf names no account on this host"
fi
if [[ "${ENTRY%%:*}" != "$OWNER" ]]; then
  fail "5 (window)" "the OWNER in bootstrap.conf is a uid. Give the account name"
fi

as_owner() {
  sudo -u "$OWNER" -H "$@"
}

WINDOW_RC=0
WINDOW_OUT="$(as_owner "$PYTHON" -m lake.deploy_window)" || WINDOW_RC=$?
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
  *) fail "5 (window)" "python -m lake.deploy_window exited $WINDOW_RC" ;;
esac
if [[ $LINES != 2 || -z "$WINDOW_LINE" || ! "$NEXT_SPAN_START" =~ $NUMBER_RE ]]; then
  fail "5 (window)" "python -m lake.deploy_window printed no next_span_start line"
fi
if [[ $WINDOW_RC == 3 ]]; then
  refuse "5 (window)" "inside the scheduled jobs' hours, where $WINDOW_LINE"
fi

# -- 6. deploy -------------------------------------------------------------------------

# A missing unit reads inactive. A systemctl that cannot answer prints nothing, which is
# an error, and any state but inactive or failed, activating included, refuses.
deploy_check() {
  local state
  state="$(systemctl is-active "$DEPLOY_SERVICE" 2>/dev/null)" || true
  case "$state" in
    inactive|failed) ;;
    "") fail "6 (deploy)" "systemctl is-active $DEPLOY_SERVICE printed no state" ;;
    *) refuse "6 (deploy)" "$1$DEPLOY_SERVICE is $state" ;;
  esac
}

deploy_check ""
mkdir -p "$ROOT/run" || fail "6 (deploy)" "could not create /run"
exec 8>"$ROOT$DEPLOY_LOCK" || fail "6 (deploy)" "could not open $DEPLOY_LOCK"
if ! flock -n 8; then
  refuse "6 (deploy)" "a deploy holds $DEPLOY_LOCK"
fi
exec 9>"$ROOT$INSTALL_LOCK" || fail "6 (deploy)" "could not open $INSTALL_LOCK"
if ! flock -n 9; then
  refuse "6 (deploy)" "an install holds $INSTALL_LOCK"
fi
# A deploy that started between the first check and the locks.
deploy_check "after the locks, "

# -- the ping and the poweroff ---------------------------------------------------------

say "every check passed, so pinging $SLUG and powering off"
PING=(-m lake.control_plane ping "$SLUG")
if [[ -n "${MARKETLAKE_CONFIG:-}" ]]; then
  PING+=(--config "$MARKETLAKE_CONFIG")
fi
PING_RC=0
as_owner "$PYTHON" "${PING[@]}" || PING_RC=$?
if [[ $PING_RC != 0 ]]; then
  say "the $SLUG ping exited $PING_RC, and the VM powers off anyway"
fi
say "powering off"
if ! systemctl poweroff; then
  fail "poweroff" "systemctl poweroff failed"
fi
