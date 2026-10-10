#!/bin/bash
# Marketlake: ask the hosted VM to deploy one commit, from GitHub Actions (#676).
#
# .github/workflows/deploy.yml runs this on the runner, after the owner approves the run
# in the `deploy` environment and the job has assumed the deploy role. It reads:
#
#   GITHUB_SHA         the commit to deploy, 40 hex digits
#   DEPLOY_DOCUMENT    the SSM document to send, set once in the workflow's env:
#   DEPLOY_TAG_KEY     the instance tag that marks the VM, set the same way
#   DEPLOY_TAG_VALUE   that tag's value
#   GITHUB_STEP_SUMMARY  where the summary goes, when it is set
#
# In order, it:
#
#   1. finds the one instance that carries the tag, in the states infra/README.md's
#      "Find the VM's address" lists, and sends nothing unless exactly one matches. A
#      terminated instance left behind by a replacement is not counted. It sends only
#      when that instance is running. The VM stops itself each night (#868), so a
#      stopped or starting VM ends the run with a line that says how to start it and to
#      re-run the workflow once it runs. It sends by instance id, because a command sent
#      to a tag reports Success when nothing matches;
#   2. sends the document with sha and notAfter, the send time plus 600 seconds, and a
#      delivery timeout of the same 600 seconds. The host refuses a start after notAfter
#      with exit 3, so a command delivered late never deploys;
#   3. polls GetCommandInvocation until the command ends. It waits through Pending,
#      InProgress, Delayed and Cancelling, and retries InvocationDoesNotExist, which
#      comes just after the send, until notAfter;
#   4. reads StatusDetails, ResponseCode and the last non-empty line of the host's
#      output, and maps them to one summary.
#
# The repository is public, so this prints only that summary, StatusDetails and
# ResponseCode, on stdout and in the step summary. It never prints the host's output,
# an AWS reply, or an AWS error message, which can carry an ARN with the account id. An
# AWS error contributes its error code alone. The full output stays in
# /var/lib/marketlake/deploy.log on the VM.
#
# It exits with the host's code, 0 to 3, only when the output ends on a line from the
# host's table and the code is one that line comes with, and with 1 otherwise. So exit 0
# needs a deployed line naming GITHUB_SHA. A stale run, whose commit main has moved past, is
# the workflow's to skip, not this script's.
#
# Written for bash 3.2 as well as a current bash, because the test suite runs it on a Mac.
set -euo pipefail

# The delivery window, which deploy.yml's timeout-minutes counts.
DELIVERY_SECONDS=600
POLL_SECONDS=15

export AWS_PAGER=""

SHA="${GITHUB_SHA:-}"
STATUS=none
CODE=none

# finish <exit code> <summary>
finish() {
  printf 'send-deploy: %s\n' "$2"
  printf 'send-deploy: StatusDetails %s, ResponseCode %s\n' "$STATUS" "$CODE"
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
      printf '### Deploy of %s\n\n' "$SHA"
      printf '%s\n\n' "$2"
      printf 'StatusDetails %s, ResponseCode %s\n' "$STATUS" "$CODE"
    } >> "$GITHUB_STEP_SUMMARY"
  fi
  exit "$1"
}

UNKNOWN="outcome unknown: compare HEAD with /var/lib/marketlake/deployed on the VM, and read /var/lib/marketlake/deploy.log"

if [[ ! "$SHA" =~ ^[0-9a-f]{40}$ ]]; then
  SHA=invalid
  finish 1 "not sent: GITHUB_SHA is not a 40-digit commit"
fi
for name in DEPLOY_DOCUMENT DEPLOY_TAG_KEY DEPLOY_TAG_VALUE; do
  if [[ -z "${!name:-}" ]]; then
    finish 1 "not sent: $name is empty"
  fi
done

# A template, because a Mac's mktemp ignores TMPDIR without one.
WORK="$(mktemp -d "${TMPDIR:-/tmp}/send-deploy.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

# The error code an AWS CLI error names, such as AccessDeniedException, and nothing else
# from the message.
ERROR_RE='An error occurred \(([A-Za-z0-9.]+)\)'
error_code() {
  local text=""
  if [[ -f "$WORK/err" ]]; then
    text="$(< "$WORK/err")"
  fi
  if [[ "$text" =~ $ERROR_RE ]]; then
    echo "${BASH_REMATCH[1]}"
  else
    echo "an error with no code"
  fi
}

# -- 1. the instance ----------------------------------------------------------------

# Each instance comes back as its id and its state on one line.
if ! pairs="$(aws ec2 describe-instances \
  --filters "Name=tag:$DEPLOY_TAG_KEY,Values=$DEPLOY_TAG_VALUE" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query 'Reservations[].Instances[].[InstanceId,State.Name]' --output text 2> "$WORK/err")"; then
  finish 1 "not sent: DescribeInstances failed with $(error_code)"
fi
set -f
# shellcheck disable=SC2086 # the pairs are split on whitespace on purpose
set -- $pairs
set +f
if [[ $(($# % 2)) -ne 0 ]]; then
  finish 1 "not sent: DescribeInstances returned something that is not an instance id and its state"
fi
if [[ $# -ne 2 ]]; then
  finish 1 "not sent: $(($# / 2)) instances carry the tag $DEPLOY_TAG_KEY = $DEPLOY_TAG_VALUE, and a deploy needs exactly one"
fi
INSTANCE="$1"
INSTANCE_STATE="$2"
if [[ ! "$INSTANCE" =~ ^i-[0-9a-f]+$ || ! "$INSTANCE_STATE" =~ ^[a-z-]+$ ]]; then
  finish 1 "not sent: DescribeInstances returned something that is not an instance id and its state"
fi
FIND="infra/README.md's \"Find the VM's address\" finds its id and state"
START="\"Start a stopped instance\" starts it"
case "$INSTANCE_STATE" in
  running) ;;
  pending)
    finish 1 "not sent: the VM is still starting. $FIND. Re-run this workflow once it runs" ;;
  stopping | stopped)
    finish 1 "not sent: the VM is stopped. $FIND, and $START. Re-run this workflow once it runs" ;;
  *)
    finish 1 "not sent: the VM is $INSTANCE_STATE, not running. $FIND" ;;
esac

# -- 2. the send --------------------------------------------------------------------

NOT_AFTER=$(($(date +%s) + DELIVERY_SECONDS))
PARAMETERS="{\"sha\":[\"$SHA\"],\"notAfter\":[\"$NOT_AFTER\"]}"
if ! COMMAND_ID="$(aws ssm send-command \
  --document-name "$DEPLOY_DOCUMENT" \
  --instance-ids "$INSTANCE" \
  --parameters "$PARAMETERS" \
  --timeout-seconds "$DELIVERY_SECONDS" \
  --query Command.CommandId --output text 2> "$WORK/err")"; then
  finish 1 "not sent: SendCommand failed with $(error_code)"
fi
# The call succeeded, so the command may be on its way even when its id is unreadable.
if [[ ! "$COMMAND_ID" =~ ^[0-9a-f-]{36}$ ]]; then
  finish 1 "$UNKNOWN. SendCommand returned no readable command id"
fi
printf 'send-deploy: sent %s to the VM, and waiting for it to end\n' "$SHA"

# -- 3. the wait --------------------------------------------------------------------

while :; do
  if result="$(aws ssm get-command-invocation \
    --command-id "$COMMAND_ID" --instance-id "$INSTANCE" \
    --query '[StatusDetails,ResponseCode]' --output text 2> "$WORK/err")"; then
    STATUS=""
    CODE=""
    read -r STATUS CODE <<< "$result" || true
    # Only a word and an integer are printed, whatever the reply held.
    if [[ ! "$STATUS" =~ ^[A-Za-z]+$ ]]; then STATUS=unreadable; fi
    if [[ ! "$CODE" =~ ^-?[0-9]+$ ]]; then CODE=unreadable; fi
    case "$STATUS" in
      Pending | InProgress | Delayed | Cancelling)
        sleep "$POLL_SECONDS"
        continue
        ;;
    esac
    break
  fi
  error="$(error_code)"
  if [[ "$error" == InvocationDoesNotExist && "$(date +%s)" -lt "$NOT_AFTER" ]]; then
    sleep "$POLL_SECONDS"
    continue
  fi
  finish 1 "$UNKNOWN. GetCommandInvocation failed with $error"
done

# -- 4. the outcome -----------------------------------------------------------------

# One pattern per row of the host's table of last lines, and beside it the exit codes
# that row comes with. A <sha> is 40 hex digits, and a <reason> is any text. Exit 0 comes
# only with a deployed line for the sha this run sent, bare or already current, and the
# host prints that line with no other code. Exits 2 and 3 come only with not deployed.
SHA_RE='[0-9a-f]{40}'
SUFFIXES=", with a failed step in deploy\\.log(, but the dashboard did not restart)?|, but the dashboard did not restart"
HOST_LINES=(
  "^deployed: $SHA( \\(already current\\))?\$"
  "^deployed: $SHA_RE( \\(already current\\))?($SUFFIXES)\$"
  "^rolled back to $SHA_RE: .+\$"
  "^rollback to $SHA_RE failed: .+\$"
  "^not restarted: the tree is at $SHA_RE, and the last recorded deploy is ($SHA_RE|none)\$"
  "^not deployed: .+\$"
  "^outcome unknown: read deploy\\.log\$"
)
HOST_CODES=("0" "1" "1" "1" "1" "1 2 3" "1")

HOST_LINE=""
LINE_CODES=""
if output="$(aws ssm get-command-invocation \
  --command-id "$COMMAND_ID" --instance-id "$INSTANCE" \
  --query StandardOutputContent --output text 2> "$WORK/err")"; then
  last=""
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    if [[ -n "$line" ]]; then
      last="$line"
    fi
  done <<< "$output"
  for i in "${!HOST_LINES[@]}"; do
    pattern="${HOST_LINES[$i]}"
    if [[ "$last" =~ $pattern ]]; then
      HOST_LINE="$last"
      LINE_CODES="${HOST_CODES[$i]}"
      break
    fi
  done
fi

# The host's line counts only beside the status and code it exits with. Any other pairing
# falls through to the rows below, and from there to outcome unknown.
if [[ -n "$HOST_LINE" && " $LINE_CODES " == *" $CODE "* ]]; then
  case "$STATUS:$CODE" in
    Success:0 | Failed:1 | Failed:2 | Failed:3) finish "$CODE" "$HOST_LINE" ;;
  esac
fi
if [[ "$STATUS" == Failed && "$CODE" == 127 ]]; then
  finish 1 "vm-deploy.sh is missing on the VM, so run the manual first deploy"
fi
if [[ "$CODE" == -1 ]]; then
  case "$STATUS" in
    DeliveryTimedOut | Undeliverable | Terminated | AccessDenied | InvalidPlatform)
      finish 1 "not delivered, so re-run"
      ;;
    Cancelled)
      finish 1 "not started"
      ;;
  esac
fi
finish 1 "$UNKNOWN"
