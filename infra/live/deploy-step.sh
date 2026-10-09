#!/bin/sh
# The one step of the SSM document marketlake-deploy, which infra/live/deploy.tf reads
# into the document with file() rather than templatefile(), so OpenTofu substitutes
# nothing here. The SSM agent substitutes the document's two parameters, which their
# allowedPattern limits to a 40-digit hex sha and 10 decimal digits, and runs the step as
# root with sh, which is dash on Ubuntu. So this is POSIX sh, not bash.
#
# The agent starts a step in its own orchestration directory with its own PATH. The step
# sets both before anything else, so deploy/vm-deploy.sh runs the same way from here as
# from a hand run.
#
# It finds the checkout the way the user-data shim made it: the owner from
# /etc/marketlake/bootstrap.conf, read line by line and never sourced, then the owner's
# home from getent. Any refusal here prints one line on stdout and exits 2, which
# deploy/send-deploy.sh reports as the host's line. That line lands in a public log, so
# it holds only this script's own words and never a value read from the conf. A checkout without vm-deploy.sh makes
# the exec below exit 127, which send-deploy.sh reports as a VM that needs the manual
# first deploy. docs/design.md's "Infrastructure, defined" carries the reasoning.
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export PATH
cd / || exit 2

not_deployed() {
  echo "not deployed: $*"
  exit 2
}

conf=/etc/marketlake/bootstrap.conf
if [ ! -f "$conf" ] || [ ! -r "$conf" ]; then
  not_deployed "$conf is missing, so the deploy cannot find the checkout"
fi

owner=""
while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in
    "") ;;
    OWNER=*)
      if [ -n "$owner" ]; then
        not_deployed "$conf sets OWNER twice"
      fi
      owner="${line#OWNER=}"
      ;;
    LAKE_VOLUME_ID=*) ;;
    *) not_deployed "$conf holds a line other than OWNER and LAKE_VOLUME_ID" ;;
  esac
done < "$conf"

# The account rule vm-bootstrap.sh applies to the same line.
case "$owner" in
  [A-Za-z_]*) ;;
  *) not_deployed "$conf does not set OWNER to an account name" ;;
esac
case "$owner" in
  *[!A-Za-z0-9_.-]*) not_deployed "$conf does not set OWNER to an account name" ;;
esac

entry="$(getent passwd "$owner")" || entry=""
# getent resolves a uid as well as a name, so the entry must start with the name.
case "$entry" in
  "$owner":*) ;;
  *) not_deployed "the OWNER in bootstrap.conf names no account on this host" ;;
esac
home="$(printf '%s\n' "$entry" | cut -d: -f6)"
if [ -z "$home" ]; then
  not_deployed "the OWNER in bootstrap.conf has no home directory"
fi

# The sha goes last, so a stray newline after it would cut off nothing. Were --not-after
# last, a newline after the sha would start a deploy with no expiry.
# shellcheck disable=SC1083 # the agent replaces both placeholders before the step runs
exec "$home/marketlake/deploy/vm-deploy.sh" --not-after {{ notAfter }} --sha {{ sha }}
