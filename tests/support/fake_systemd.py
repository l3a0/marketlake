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
   while a unit file differs from the copy taken at the last ``daemon-reload``.
6. ``start`` of a ``.mount`` unit mounts the fake disk in ``tests.support.fake_disk``,
   succeeds again while it is mounted, and fails for a unit named in ``FAIL_START``.
7. ``list-units`` prints one plain line per ``com.marketlake.*`` unit with its active
   state, and a started timer reads ``active waiting``.

``FAIL_START`` names units whose interpreter cannot start. ``RESTART_MODE`` picks what a
restart does: ``new`` brings a new pid, ``same`` leaves the pid alone, ``never`` leaves no
process, and ``crash`` hands out a fresh pid on every read. ``RESTART_DELAY`` is how many
``MainPID`` reads after a restart answer 0 before the new pid shows. ``FAIL_SHOW`` names
units whose ``show`` exits 1, as a D-Bus timeout makes it.

Fakes also stand in for ``id``, ``getent``, ``sudo -u`` and ``flock``, the last two of
which macOS lacks, and for ``git`` and ``sleep``. Each logs its argv to ``$LOG``. The
fake ``getent`` answers the owner's uid, 1000, as well as the name, as glibc's does.
"""

from __future__ import annotations

from pathlib import Path

FAKE_SYSTEMCTL = r"""#!/bin/bash
printf 'systemctl %s\n' "$*" >> "$LOG"
UNIT_DIR="${MARKETLAKE_INSTALL_ROOT}/etc/systemd/system"
# Builtins rather than tools wherever bash has one, so each call starts few processes.
if [[ ! -d "$STATE/pending" ]]; then
  mkdir -p "$STATE/reloaded" "$STATE/enabled" "$STATE/pid" "$STATE/failed" "$STATE/pending"
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
  if [[ "${RESTART_MODE:-new}" == crash && -f "$STATE/restarted-$1" ]]; then
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
      if [[ -f "$STATE/pid/$unit" ]]; then echo active
      elif [[ -f "$STATE/failed/$unit" ]]; then echo failed
      else echo inactive; fi ;;
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
    # each, as --all --no-legend --plain prints them. A started timer reads as waiting.
    [[ -n "${LIST_UNITS_RC:-}" ]] && exit "$LIST_UNITS_RC"
    names=""
    for f in "$UNIT_DIR"/com.marketlake.* "$STATE"/pid/com.marketlake.* \
        "$STATE"/failed/com.marketlake.*; do
      if [[ -e "$f" ]]; then names="$names ${f##*/}"; fi
    done
    for name in $(printf '%s\n' $names | sort -u); do
      if [[ -f "$STATE/pid/$name" ]]; then
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
    for arg in "$@"; do
      case "$arg" in
        --value) value=1 ;;
        --property=*) props="${arg#--property=}" ;;
        *) unit="$arg" ;;
      esac
    done
    for name in ${props//,/ }; do
      if [[ $value == 1 ]]; then
        property "$unit" "$name"
      else
        echo "$name=$(property "$unit" "$name")"
      fi
    done
    exit 0 ;;
esac
exit 0
"""

FAKE_ID = """#!/bin/bash
printf 'id %s\\n' "$*" >> "$LOG"
if [[ "$1" == "-u" ]]; then echo "${FAKE_UID:-0}"; exit 0; fi
exit 1
"""

# sudo -u <account> -H runs the rest as that account. The fake logs the whole line and
# runs the command, so the log shows which commands ran through it.
FAKE_SUDO = """#!/bin/bash
printf 'sudo %s\\n' "$*" >> "$LOG"
while [[ $# -gt 0 ]]; do
  case "$1" in
    -u) shift 2 ;;
    -H|-n) shift ;;
    *) break ;;
  esac
done
exec "$@"
"""

# glibc's getent passwd resolves a uid as well as a name, so this answers both.
FAKE_GETENT = """#!/bin/bash
printf 'getent %s\\n' "$*" >> "$LOG"
if [[ "$1" == "passwd" && ( "$2" == "$FAKE_OWNER" || "$2" == 1000 ) ]]; then
  echo "$FAKE_OWNER:x:1000:1000:Some One:$FAKE_HOME:/bin/bash"
  exit 0
fi
exit 2
"""

FAKE_FLOCK = """#!/bin/bash
printf 'flock %s\\n' "$*" >> "$LOG"
exit "${FLOCK_RC:-0}"
"""

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

FAKES = {
    "systemctl": FAKE_SYSTEMCTL,
    "id": FAKE_ID,
    "sudo": FAKE_SUDO,
    "getent": FAKE_GETENT,
    "flock": FAKE_FLOCK,
    "git": FAKE_GIT,
    "sleep": FAKE_SLEEP,
}


def install_fakes(bin_dir: Path) -> None:
    """Write every fake into ``bin_dir``, executable."""
    bin_dir.mkdir(parents=True, exist_ok=True)
    for name, body in FAKES.items():
        path = bin_dir / name
        path.write_text(body)
        path.chmod(0o755)
