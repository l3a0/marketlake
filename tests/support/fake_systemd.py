"""Stand-ins for the tools the systemd scripts call, so a test runs them without root.

The rendered ``install.sh``, ``uninstall.sh`` and ``restart.sh``, and the tracked
``deploy/linux-install.sh``, run here against fakes on ``PATH`` with
``MARKETLAKE_INSTALL_ROOT`` pointed at a temporary directory. Every system path the
scripts write lands under that prefix, so a test observes each write on a Mac as well as
on CI's Linux runner.

The fake ``systemctl`` is the one that matters. It keeps each unit's state in a directory
and refuses what systemd 255 refuses, so a step that would abort on a real host aborts in
a test too.

1. ``disable`` of a unit whose file is missing exits 1.
2. ``stop`` of a unit that is not loaded exits 5. A unit is loaded when its file is in the
   unit directory or systemd read it at the last ``daemon-reload``.
3. ``enable --now`` exits 0 whatever the start does, as systemd 255's does.
4. ``restart`` of a unit that cannot start exits 1, as ``Type=exec`` makes it.
5. ``show`` answers each property the scripts read. ``NeedDaemonReload`` reads ``yes``
   while a unit file differs from the copy taken at the last ``daemon-reload``, and
   ``ControlGroup`` names the unit's slice while it runs. ``show -p A,B`` takes a comma
   list and prints the properties in the reverse of the order asked, as a real
   ``systemctl`` may print them in its own, so a caller must match each by name.
6. ``start`` of a ``.mount`` unit mounts the fake disk in ``tests.support.fake_disk``,
   succeeds again while it is mounted, and fails for a unit named in ``FAIL_START``.
7. ``list-units`` prints one plain line per ``com.marketlake.*`` unit with its active
   state, and a started timer reads ``active waiting``. ``--type=service`` leaves the
   timers out.
8. ``is-active`` exits 0 for a running unit and 3 otherwise, printing the state unless
   ``--quiet`` is given.

A unit whose name is a file in ``$STATE/activating`` reads ``activating``, as a oneshot
does while it runs, until a ``restart`` of it. A unit named in ``$STATE/crashing`` hands
out a fresh pid on every ``MainPID`` read, as a daemon that dies and restarts does.

``FAIL_START`` names units whose interpreter cannot start. ``RESTART_MODE`` picks what a
restart does: ``new`` brings a new pid, ``same`` leaves the pid alone, ``never`` leaves no
process, and ``crash`` hands out a fresh pid on every read. ``RESTART_DELAY`` is how many
``MainPID`` reads after a restart answer 0 before the new pid shows. ``FAIL_SHOW`` names
units whose ``show`` exits 1, as a D-Bus timeout makes it.

Fakes also stand in for ``id``, ``getent``, ``sudo -u`` and ``flock``, the last two of
which macOS lacks, and for ``git``, ``sleep`` and ``loginctl``. The fake ``loginctl``
prints ``FAKE_SESSIONS`` and exits ``LOGINCTL_RC``. Each logs its argv to ``$LOG``. The
fake ``flock`` takes both of its forms. With a bare descriptor, as ``flock -w 600 9``, it
exits the next code in ``FLOCK_RCS`` when that is set, one per call, and ``FLOCK_RC``
otherwise. With a file and a command, as ``flock -w 600 <file> <command>``, it
creates the file and runs the command, unless the next code in ``FLOCK_FILE_RCS`` is not
0, which it exits with instead, as a lock held for the whole wait makes the real one. The
fake ``getent`` answers the owner's uid, 1000, as well as the name, as glibc's does. Its
home field is ``FAKE_GETENT_HOME`` when that is set, even to nothing, and ``FAKE_HOME``
otherwise. Its name field is ``FAKE_GETENT_NAME`` when that is set, as a directory that
matches names without regard to case answers with the canonical one. The
fake ``sudo`` clears the environment as the real one does. It keeps only ``PATH``,
``LOG``, ``STATE``, ``TOOLS``, every ``FAKE_*`` variable, the exit-code knobs named
``*_RC`` and ``*_RCS``, and the fake ``git``'s ``IS_REPO``, ``BRANCH`` and ``DIRTY``. It
sets ``HOME`` from ``FAKE_HOME``.

``DEPLOY_FAKES`` adds what ``deploy/vm-deploy.sh`` calls and a Mac lacks.

1. ``timeout`` runs its command, unless ``FAKE_TIMEOUT_EXPIRES`` names the command's
   file name, which it answers with 124, as a command that ran out of time.
   ``FAKE_TIMEOUT_BUSY`` names a service it leaves activating before the command runs,
   as a timer job that starts during the deploy's fetch would be.
2. ``systemd-run`` starts its command with only the ``--setenv`` pairs, systemd's default
   ``PATH`` where no pair sets one, the harness variables the fake ``sudo`` keeps, and the
   fake ``systemctl``'s knobs, so a variable the caller forgot to pass is missing, as on a
   real host. It sets
   ``INVOCATION_ID``, marks the unit active while the command runs, and keeps the
   command's pid in ``$STATE/inner-pid``. Output goes to the files the ``-p
   StandardOutput=append:`` and ``StandardError=append:`` properties name, and to
   ``$STATE/journal`` when none is named. After the command exits it copies the result
   file the ``MARKETLAKE_DEPLOY_RESULT`` pair names to ``$STATE/last-result``, and keeps
   the command's exit code in ``$STATE/inner-rc``.
   ``FAKE_SYSTEMD_RUN=taken`` refuses the start as a unit another caller started first,
   leaving the unit active, ``FAKE_SYSTEMD_RUN=fail`` refuses it with nothing active,
   and ``FAKE_SYSTEMD_RUN=result`` runs nothing and writes ``FAKE_RESULT`` as the result,
   with ``printf %b``'s escapes. ``FAKE_BROKEN_PIPE`` ignores SIGPIPE, as systemd does by
   default, and points the command's output at a pipe whose reader has exited.
3. ``mv`` exits ``MV_RC`` when it is set, and otherwise runs the real tool.

``FAKE_NEEDRESTART`` is a fake ``needrestart``, installed in a directory of its own so a
test can leave it off ``PATH``. It prints a ``NEEDRESTART-SVC`` line for each service in
``FAKE_NEEDRESTART_SVC`` and exits ``NEEDRESTART_RC``.

Each fake is a symlink to the one program in ``tests.support.fake_bin``, which sources the
fake's body, so a Mac scans one new file per process rather than one per fake.
"""

from __future__ import annotations

from pathlib import Path

from tests.support.fake_bin import install

FAKE_SYSTEMCTL = r"""#!/bin/bash
printf 'systemctl %s\n' "$*" >> "$LOG"
UNIT_DIR="${MARKETLAKE_INSTALL_ROOT}/etc/systemd/system"
# Builtins rather than tools wherever bash has one, so each call starts few processes.
if [[ ! -d "$STATE/crashing" ]]; then
  mkdir -p "$STATE/reloaded" "$STATE/enabled" "$STATE/pid" "$STATE/failed" "$STATE/pending" \
    "$STATE/activating" "$STATE/crashing"
fi

fails_to_start() {
  case " ${FAIL_START:-} " in *" $1 "*) return 0 ;; esac
  return 1
}

loaded() {
  [[ -f "$UNIT_DIR/$1" || -f "$STATE/reloaded/$1" ]]
}

next_pid() {
  local n
  n=1000
  if [[ -f "$STATE/pidseq" ]]; then n="$(<"$STATE/pidseq")"; fi
  n=$((n + 1))
  printf '%s' "$n" > "$STATE/pidseq"
  printf '%s' "$n"
}

start() {
  if [[ -f "$STATE/pid/$1" ]]; then return 0; fi
  echo "start $1" >> "$STATE/events"
  if fails_to_start "$1"; then
    : > "$STATE/failed/$1"
    return 1
  fi
  rm -f "$STATE/failed/$1"
  next_pid > "$STATE/pid/$1"
}

need_reload() {
  if [[ -f "$UNIT_DIR/$1" && -f "$STATE/reloaded/$1" ]]; then
    cmp -s "$UNIT_DIR/$1" "$STATE/reloaded/$1" && echo no || echo yes
  elif [[ -f "$UNIT_DIR/$1" || -f "$STATE/reloaded/$1" ]]; then
    echo yes
  else
    echo no
  fi
}

main_pid() {
  local pending
  pending=0
  if [[ -f "$STATE/pending/$1" ]]; then pending="$(<"$STATE/pending/$1")"; fi
  if [[ "$pending" -gt 0 ]]; then
    printf '%s' "$((pending - 1))" > "$STATE/pending/$1"
    echo 0
    return
  fi
  if [[ ! -f "$STATE/pid/$1" ]]; then
    echo 0
    return
  fi
  if [[ ( "${RESTART_MODE:-new}" == crash && -f "$STATE/restarted-$1" ) \
      || -f "$STATE/crashing/$1" ]]; then
    next_pid > "$STATE/pid/$1"
  fi
  echo "$(<"$STATE/pid/$1")"
}

property() {
  local unit="$1" name="$2"
  case "$name" in
    LoadState) loaded "$unit" && echo loaded || echo not-found ;;
    MainPID) main_pid "$unit" ;;
    NeedDaemonReload) need_reload "$unit" ;;
    ActiveState)
      if [[ -f "$STATE/activating/$unit" ]]; then echo activating
      elif [[ -f "$STATE/pid/$unit" ]]; then echo active
      elif [[ -f "$STATE/failed/$unit" ]]; then echo failed
      else echo inactive; fi ;;
    ControlGroup)
      if [[ -f "$STATE/pid/$unit" ]]; then echo "/system.slice/$unit"; else echo ""; fi ;;
    SubState)
      if [[ -f "$STATE/pid/$unit" ]]; then echo running
      elif [[ -f "$STATE/failed/$unit" ]]; then echo failed
      else echo dead; fi ;;
    NRestarts) echo 0 ;;
    Result) [[ -f "$STATE/failed/$unit" ]] && echo exit-code || echo success ;;
    ExecMainStatus) [[ -f "$STATE/failed/$unit" ]] && echo 203 || echo 0 ;;
    *) echo "" ;;
  esac
}

cmd="$1"
shift
now=0
if [[ "${1:-}" == "--now" ]]; then now=1; shift; fi
case "$cmd" in
  daemon-reload)
    rm -rf "$STATE/reloaded"
    mkdir -p "$STATE/reloaded"
    for f in "$UNIT_DIR"/*.service "$UNIT_DIR"/*.timer; do
      if [[ -f "$f" ]]; then cp "$f" "$STATE/reloaded/"; fi
    done
    exit 0 ;;
  stop)
    if ! loaded "$1"; then echo "Failed to stop $1: Unit $1 not loaded." >&2; exit 5; fi
    rm -f "$STATE/pid/$1"
    exit 0 ;;
  disable)
    if [[ ! -f "$UNIT_DIR/$1" ]]; then
      echo "Failed to disable unit: Unit file $1 does not exist." >&2
      exit 1
    fi
    rm -f "$STATE/enabled/$1"
    if [[ $now == 1 ]]; then rm -f "$STATE/pid/$1"; fi
    exit 0 ;;
  enable)
    if [[ ! -f "$UNIT_DIR/$1" ]]; then
      echo "Failed to enable unit: Unit file $1 does not exist." >&2
      exit 1
    fi
    : > "$STATE/enabled/$1"
    if [[ $now == 1 ]]; then start "$1" || true; fi
    exit 0 ;;
  restart)
    if ! loaded "$1"; then echo "Failed to restart $1: Unit $1 not found." >&2; exit 5; fi
    echo "restart $1" >> "$STATE/events"
    rm -f "$STATE/activating/$1"
    case "${RESTART_MODE:-new}" in
      same) exit 0 ;;
      never) rm -f "$STATE/pid/$1"; exit 0 ;;
    esac
    rm -f "$STATE/pid/$1"
    if fails_to_start "$1"; then
      : > "$STATE/failed/$1"
      echo "Job for $1 failed because of unavailable resources or another system error." >&2
      exit 1
    fi
    next_pid > "$STATE/pid/$1"
    : > "$STATE/restarted-$1"
    printf '%s' "${RESTART_DELAY:-0}" > "$STATE/pending/$1"
    exit 0 ;;
  start)
    # A mount unit mounts the fake disk's filesystem, recording its UUID where the fake
    # findmnt reads it. Starting an active unit succeeds whatever is mounted, as
    # systemd's does, and a unit named in FAIL_START fails.
    if [[ "$1" == *.mount ]]; then
      if fails_to_start "$1"; then
        echo "Job for $1 failed. See \"systemctl status $1\" for details." >&2
        exit 1
      fi
      if [[ -f "$STATE/mounted" ]]; then exit 0; fi
      if [[ ! -f "$STATE/disk/uuid" ]]; then
        echo "Job for $1 failed: no filesystem." >&2
        exit 1
      fi
      cp "$STATE/disk/uuid" "$STATE/mounted"
      exit 0
    fi
    if ! start "$1"; then exit 1; fi
    exit 0 ;;
  list-units)
    # Every com.marketlake.* unit with a file, a process or a failure, one plain line
    # each, as --all --no-legend --plain prints them. A started timer reads as waiting,
    # and --type=service leaves the timers out.
    [[ -n "${LIST_UNITS_RC:-}" ]] && exit "$LIST_UNITS_RC"
    services=0
    for arg in "$@"; do
      if [[ "$arg" == --type=service ]]; then services=1; fi
    done
    names=""
    for f in "$UNIT_DIR"/com.marketlake.* "$STATE"/pid/com.marketlake.* \
        "$STATE"/failed/com.marketlake.* "$STATE"/activating/com.marketlake.*; do
      if [[ -e "$f" ]]; then names="$names ${f##*/}"; fi
    done
    for name in $(printf '%s\n' $names | sort -u); do
      if [[ $services == 1 && "$name" != *.service ]]; then continue; fi
      if [[ -f "$STATE/activating/$name" ]]; then
        echo "$name loaded activating start $name"
      elif [[ -f "$STATE/pid/$name" ]]; then
        sub=running; [[ "$name" == *.timer ]] && sub=waiting
        echo "$name loaded active $sub $name"
      elif [[ -f "$STATE/failed/$name" ]]; then
        echo "$name loaded failed failed $name"
      else
        echo "$name loaded inactive dead $name"
      fi
    done
    exit 0 ;;
  show)
    for arg in "$@"; do
      case " ${FAIL_SHOW:-} " in
        *" $arg "*) echo "Failed to get properties: Connection timed out" >&2; exit 1 ;;
      esac
    done
    value=0
    props=""
    unit=""
    reverse=0
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --value) value=1 ;;
        --property=*) props="${1#--property=}" ;;
        -p) props="$2"; reverse=1; shift ;;
        *) unit="$1" ;;
      esac
      shift
    done
    names=""
    for name in ${props//,/ }; do
      if [[ $reverse == 1 ]]; then names="$name $names"; else names="$names $name"; fi
    done
    for name in $names; do
      if [[ $value == 1 ]]; then
        property "$unit" "$name"
      else
        echo "$name=$(property "$unit" "$name")"
      fi
    done
    exit 0 ;;
  is-active)
    quiet=0
    unit=""
    for arg in "$@"; do
      case "$arg" in
        --quiet|-q) quiet=1 ;;
        *) unit="$arg" ;;
      esac
    done
    state="$(property "$unit" ActiveState)"
    if [[ $quiet == 0 ]]; then echo "$state"; fi
    [[ "$state" == active ]] && exit 0
    exit 3 ;;
esac
exit 0
"""

FAKE_ID = """#!/bin/bash
printf 'id %s\\n' "$*" >> "$LOG"
if [[ "$1" == "-u" ]]; then echo "${FAKE_UID:-0}"; exit 0; fi
exit 1
"""

# sudo -u <account> -H runs the rest as that account. The fake logs the whole line and
# runs the command, so the log shows which commands ran through it. It resets the
# environment as sudo's env_reset does, so a variable exported before sudo does not reach
# the command. The command keeps PATH, the harness's own variables and its knobs, and
# gets HOME from FAKE_HOME, as -H sets it to the account's home.
FAKE_SUDO = """#!/bin/bash
printf 'sudo %s\\n' "$*" >> "$LOG"
while [[ $# -gt 0 ]]; do
  case "$1" in
    -u) shift 2 ;;
    -H|-n) shift ;;
    *) break ;;
  esac
done
keep=()
for name in $(compgen -e); do
  case "$name" in
    PATH|LOG|STATE|TOOLS|FAKE_*|*_RC|*_RCS|IS_REPO|BRANCH|DIRTY) keep+=("$name=${!name}") ;;
  esac
done
if [[ -n "${FAKE_HOME:-}" ]]; then keep+=("HOME=$FAKE_HOME"); fi
exec /usr/bin/env -i "${keep[@]}" "$@"
"""

# glibc's getent passwd resolves a uid as well as a name, so this answers both.
FAKE_GETENT = """#!/bin/bash
printf 'getent %s\\n' "$*" >> "$LOG"
if [[ "$1" == "passwd" && ( "$2" == "$FAKE_OWNER" || "$2" == 1000 ) ]]; then
  name="${FAKE_GETENT_NAME:-$FAKE_OWNER}"
  echo "$name:x:1000:1000:Some One:${FAKE_GETENT_HOME-$FAKE_HOME}:/bin/bash"
  exit 0
fi
exit 2
"""

# A sequence of exit codes, one per call, read from a space-separated list. The last code
# repeats once the list runs out, and an empty list answers 0. The count lives in $STATE,
# so a sequence survives across processes.
NEXT_RC = r"""next_rc() {
  local name="$1" list="$2" n=0 i=0 rc=0 code
  if [[ -f "$STATE/count-$name" ]]; then n="$(<"$STATE/count-$name")"; fi
  printf '%s' "$((n + 1))" > "$STATE/count-$name"
  for code in $list; do
    rc="$code"
    if [[ $i == "$n" ]]; then break; fi
    i=$((i + 1))
  done
  echo "$rc"
}
"""

# Both forms of flock: a bare descriptor answers the next code in FLOCK_RCS, or FLOCK_RC
# when no sequence is set, and a file with a command runs the command, unless
# FLOCK_FILE_RCS says the lock stayed held.
FAKE_FLOCK = (
    "#!/bin/bash\n"
    'printf \'flock %s\\n\' "$*" >> "$LOG"\n'
    + NEXT_RC
    + r"""while [[ $# -gt 0 ]]; do
  case "$1" in
    -w|-E) shift 2 ;;
    -*) shift ;;
    *) break ;;
  esac
done
if [[ $# -le 1 ]]; then
  if [[ -n "${FLOCK_RCS:-}" ]]; then exit "$(next_rc flock-fd "$FLOCK_RCS")"; fi
  exit "${FLOCK_RC:-0}"
fi
rc="$(next_rc flock-file "${FLOCK_FILE_RCS:-}")"
if [[ "$rc" != 0 ]]; then exit "$rc"; fi
: >> "$1"
shift
exec "$@"
"""
)

FAKE_GIT = """#!/bin/bash
printf 'git %s\\n' "$*" >> "$LOG"
case "$*" in
  *rev-parse*--git-dir*)      [[ "${IS_REPO:-1}" == "1" ]] && exit 0 || exit 128 ;;
  *rev-parse*--abbrev-ref*)   echo "${BRANCH:-main}"; exit 0 ;;
  *status*--porcelain*)       [[ "${DIRTY:-0}" == "1" ]] && echo " M src/lake/x.py"; exit 0 ;;
esac
exit 0
"""

FAKE_SLEEP = "#!/bin/bash\nexit 0\n"

# loginctl list-sessions --no-legend, which deploy/vm-stop.sh reads. It prints
# FAKE_SESSIONS through printf %b, one session a line in systemd 255's columns: SESSION,
# UID, USER, SEAT, TTY, STATE, IDLE and SINCE, with - for an empty cell. It exits
# LOGINCTL_RC.
FAKE_LOGINCTL = r"""#!/bin/bash
printf 'loginctl %s\n' "$*" >> "$LOG"
if [[ -n "${FAKE_SESSIONS:-}" ]]; then printf '%b' "$FAKE_SESSIONS"; fi
exit "${LOGINCTL_RC:-0}"
"""

FAKES = {
    "systemctl": FAKE_SYSTEMCTL,
    "id": FAKE_ID,
    "sudo": FAKE_SUDO,
    "getent": FAKE_GETENT,
    "flock": FAKE_FLOCK,
    "git": FAKE_GIT,
    "sleep": FAKE_SLEEP,
    "loginctl": FAKE_LOGINCTL,
}


# The deploy's fakes, which ``install_deploy_fakes`` adds to a directory of the others.

FAKE_TIMEOUT = r"""#!/bin/bash
printf 'timeout %s\n' "$*" >> "$LOG"
while [[ $# -gt 0 ]]; do
  case "$1" in
    -k|-s) shift 2 ;;
    -*) shift ;;
    *) break ;;
  esac
done
shift
case " ${FAKE_TIMEOUT_EXPIRES:-} " in
  *" ${1##*/} "*) exit 124 ;;
esac
if [[ -n "${FAKE_TIMEOUT_BUSY:-}" ]]; then
  mkdir -p "$STATE/activating"
  : > "$STATE/activating/$FAKE_TIMEOUT_BUSY"
fi
exec "$@"
"""

FAKE_SYSTEMD_RUN = r"""#!/bin/bash
printf 'systemd-run %s\n' "$*" >> "$LOG"
unit=""
out=""
err=""
pairs=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --unit=*) unit="${1#--unit=}.service" ;;
    --setenv=*) pairs+=("${1#--setenv=}") ;;
    -p)
      case "$2" in
        StandardOutput=append:*) out="${2#StandardOutput=append:}" ;;
        StandardError=append:*) err="${2#StandardError=append:}" ;;
      esac
      shift ;;
    --wait|--collect|--quiet) ;;
    -*) echo "fake systemd-run: unexpected option $1" >&2; exit 1 ;;
    *) break ;;
  esac
  shift
done
mkdir -p "$STATE/pid"
case "${FAKE_SYSTEMD_RUN:-}" in
  result)
    # The unit ran and wrote FAKE_RESULT, whatever it holds, as its result.
    for pair in ${pairs[@]+"${pairs[@]}"}; do
      case "$pair" in
        MARKETLAKE_DEPLOY_RESULT=*) printf '%b' "$FAKE_RESULT" > "${pair#*=}" ;;
      esac
    done
    exit 0 ;;
  taken)
    printf '1' > "$STATE/pid/$unit"
    echo "Failed to start transient service unit: Unit $unit was already loaded." >&2
    exit 1 ;;
  fail)
    echo "Failed to start transient service unit: Access denied" >&2
    exit 1 ;;
esac
if [[ -f "$STATE/pid/$unit" ]]; then
  echo "Failed to start transient service unit: Unit $unit was already loaded." >&2
  exit 1
fi
# systemd's default PATH first, so only a --setenv pair puts the fakes on it.
keep=("PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
for name in $(compgen -e); do
  case "$name" in
    PATH) ;;
    LOG|STATE|TOOLS|FAKE_*|*_RC|*_RCS|IS_REPO|BRANCH|DIRTY) keep+=("$name=${!name}") ;;
    FAIL_SHOW|FAIL_START|RESTART_MODE|RESTART_DELAY) keep+=("$name=${!name}") ;;
  esac
done
keep+=(${pairs[@]+"${pairs[@]}"} "INVOCATION_ID=fake$RANDOM$RANDOM")
printf '%s' "$$" > "$STATE/pid/$unit"
if [[ -n "${FAKE_BROKEN_PIPE:-}" ]]; then
  # The reader opens the pipe and exits at once, so every write after it fails.
  trap '' PIPE
  fifo="$STATE/broken-pipe"
  rm -f "$fifo"
  mkfifo "$fifo"
  ( exec 0<"$fifo" ) &
  reader=$!
  exec 7>"$fifo"
  wait "$reader"
  rm -f "$fifo"
  /usr/bin/env -i "${keep[@]}" "$@" >&7 2>&7 &
else
  /usr/bin/env -i "${keep[@]}" "$@" >> "${out:-$STATE/journal}" 2>> "${err:-$STATE/journal}" &
fi
inner=$!
printf '%s' "$inner" > "$STATE/inner-pid"
rc=0
wait "$inner" || rc=$?
printf '%s' "$rc" > "$STATE/inner-rc"
rm -f "$STATE/pid/$unit"
for pair in ${pairs[@]+"${pairs[@]}"}; do
  case "$pair" in
    MARKETLAKE_DEPLOY_RESULT=*)
      if [[ -f "${pair#*=}" ]]; then cp "${pair#*=}" "$STATE/last-result"; fi ;;
  esac
done
exit "$rc"
"""

FAKE_MV = r"""#!/bin/bash
printf 'mv %s\n' "$*" >> "$LOG"
if [[ -n "${MV_RC:-}" ]]; then exit "$MV_RC"; fi
exec /bin/mv "$@"
"""

FAKE_NEEDRESTART = r"""#!/bin/bash
printf 'needrestart %s\n' "$*" >> "$LOG"
echo "NEEDRESTART-VER: 3.6"
for svc in ${FAKE_NEEDRESTART_SVC:-}; do
  echo "NEEDRESTART-SVC: $svc"
done
exit "${NEEDRESTART_RC:-0}"
"""

DEPLOY_FAKES = {
    "timeout": FAKE_TIMEOUT,
    "systemd-run": FAKE_SYSTEMD_RUN,
    "mv": FAKE_MV,
}


def install_fakes(bin_dir: Path) -> None:
    """Install every fake into ``bin_dir`` through ``tests.support.fake_bin``."""
    for name, body in FAKES.items():
        install(bin_dir / name, body)


def install_deploy_fakes(bin_dir: Path, needrestart_dir: Path) -> None:
    """Add the deploy's fakes to ``bin_dir``, and ``needrestart`` alone to its own directory."""
    for name, body in DEPLOY_FAKES.items():
        install(bin_dir / name, body)
    install(needrestart_dir / "needrestart", FAKE_NEEDRESTART)
