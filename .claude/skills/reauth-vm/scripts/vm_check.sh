#!/bin/bash
# Check the hosted VM's copy of the Schwab token, and optionally pull it first.
#
# Usage: vm_check.sh <address> [pull]
#
# With "pull" it runs `python -m lake.token_store pull` on the VM as ubuntu,
# which copies the SSM token parameter into the VM's token.json only when the
# parameter was minted later. A repeat writes nothing, so running it twice is
# safe. Then it prints the VM token's mint time, the last lines the Sunday job
# printed, and whether the daemon and the Sunday job are running.
#
# Nothing here prints a token byte. The pull's line names the token's path and
# two mint times, and the mint script prints times alone.
set -euo pipefail

addr="${1:?usage: vm_check.sh <address> [pull]}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ssh_vm=(ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes
  -o HostKeyAlias=marketlake-vm -i "$HOME/.ssh/marketlake_vm" "ubuntu@$addr")

if [ "${2:-}" = pull ]; then
  # -W ignore drops the import-time deprecation warning a dependency prints.
  "${ssh_vm[@]}" 'cd ~/marketlake && .venv/bin/python -W ignore -m lake.token_store pull' \
    || echo "vm_check: the pull exited $?"
fi

"${ssh_vm[@]}" python3 - < "$here/token_mint.py"

# A running or just-finished job keeps its invocation id, so its lines can be read
# apart from last week's. A unit with no id prints the journal's tail instead, which
# can mix in an earlier Sunday, so the dates in the lines say which run they are.
echo "vm_check: the Sunday job's lines"
"${ssh_vm[@]}" 'id=$(systemctl show -p InvocationID --value com.marketlake.sunday.service)
if [ -n "$id" ]; then
  journalctl _SYSTEMD_INVOCATION_ID="$id" --no-pager -o cat | grep "^sunday: "
else
  echo "(no current run; the last lines, which may be an earlier Sunday)"
  journalctl -u com.marketlake.sunday -n 30 --no-pager -o cat | grep "^sunday: " | tail -12
fi'

echo "vm_check: daemon, Sunday job"
"${ssh_vm[@]}" 'systemctl is-active com.marketlake.daemon.service com.marketlake.sunday.service' || true
