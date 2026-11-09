#!/bin/bash
# Check the hosted VM's copy of the Schwab token, and optionally pull it first.
#
# Usage: vm_check.sh <address> [pull]
#
# With "pull" it runs `python -m lake.token_store pull` on the VM as ubuntu,
# which copies the SSM token parameter into the VM's token.json when that file
# is absent or unreadable, or the parameter was minted later. A repeat writes nothing, so running it twice is
# safe. Then it prints the VM token's mint time, the Sunday job's lines with
# their Eastern times, and whether the daemon and the Sunday job are running.
# Every probe after the pull reports its own failure and the script goes on,
# because the later probes are what explain an earlier one.
#
# It exits 0 when every step worked, 1 when the pull or the mint read failed,
# and 255 when ssh could not reach the VM.
#
# Nothing here prints a token byte. The pull's line names the token's path and
# two mint times, and the mint script prints times alone.
set -uo pipefail

addr="${1:?usage: vm_check.sh <address> [pull]}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ssh_vm=(ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes
  -o HostKeyAlias=marketlake-vm -i "$HOME/.ssh/marketlake_vm" "ubuntu@$addr")
status=0

# ssh exits 255 for its own failures, and no step below exits 255 itself.
# token_store pull exits 0, 1, 2 or 3, and the mint script 0 or 1.
unreachable() {
  echo "vm_check: ssh could not reach $addr (exit 255). Look the state and address up again."
  exit 255
}

if [ "${2:-}" = pull ]; then
  # -W ignore drops the import-time deprecation warning a dependency prints.
  "${ssh_vm[@]}" 'cd ~/marketlake && .venv/bin/python -W ignore -m lake.token_store pull'
  rc=$?
  [ "$rc" -eq 255 ] && unreachable
  [ "$rc" -ne 0 ] && { echo "vm_check: the pull exited $rc"; status=1; }
fi

"${ssh_vm[@]}" python3 - < "$here/token_mint.py"
rc=$?
[ "$rc" -eq 255 ] && unreachable
[ "$rc" -ne 0 ] && { echo "vm_check: the mint read exited $rc"; status=1; }

# A running Sunday job has an invocation id, so its lines can be read apart from
# last week's. A unit with no current run prints the journal's tail instead, and
# each line's Eastern time says which evening it came from. The job prints every
# attempt's summary together when the whole job ends. Until then only a failed
# canary call prints, so a run with no line yet is normal.
echo "vm_check: the Sunday job's lines"
"${ssh_vm[@]}" 'export TZ=America/New_York
id=$(systemctl show -p InvocationID --value com.marketlake.sunday.service)
if [ -n "$id" ]; then
  journalctl _SYSTEMD_INVOCATION_ID="$id" --no-pager -o short-iso | grep ": sunday: " \
    || echo "(this run has printed no sunday: line yet)"
else
  echo "(no current run. These are the last lines, which can be an earlier Sunday)"
  journalctl -u com.marketlake.sunday -n 40 --no-pager -o short-iso | grep ": sunday: " | tail -12
fi'
[ $? -eq 255 ] && unreachable

echo "vm_check: daemon, Sunday job"
"${ssh_vm[@]}" 'systemctl is-active com.marketlake.daemon.service com.marketlake.sunday.service'
[ $? -eq 255 ] && unreachable

exit "$status"
