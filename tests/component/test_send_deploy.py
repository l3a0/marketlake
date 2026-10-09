"""``deploy/send-deploy.sh`` run against a fake ``aws`` (#676).

The script runs on a GitHub runner, where the repository's logs are public. So beyond
mapping each outcome to its summary, these tests put a sentinel in everything AWS could
hand back, the host's full output and every error message, and check that it never
reaches stdout, stderr or the step summary.

``aws``, ``date`` and ``sleep`` are fakes on ``PATH``, each a link to
``tests.support.fake_bin``'s one program. The fake ``aws`` logs its argv to ``$LOG`` and
answers by what it was asked.

- ``ec2 describe-instances`` prints ``INSTANCES``, the running instance ids that carry the
  tag, or fails with ``DESCRIBE_ERROR``.
- ``ssm send-command`` prints a command id, or fails with ``SEND_ERROR``.
- ``ssm get-command-invocation`` with ``--query StandardOutputContent`` prints the file
  ``HOST_OUTPUT``, or fails with ``OUTPUT_ERROR``. With any other query it answers the
  next entry of ``INVOCATIONS``, a space-separated list in which ``DNE`` is an
  ``InvocationDoesNotExist`` error, ``ERR`` a throttling error, and anything else is
  ``<StatusDetails>:<ResponseCode>``. The last entry repeats.

``date +%s`` answers the next entry of ``DATES`` the same way, and ``sleep`` only logs.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

from tests.support.fake_bin import install
from tests.support.fake_disk import FAKE_SLEEP, NEXT_RC

REPO_ROOT = Path(__file__).resolve().parents[2]
SEND = REPO_ROOT / "deploy" / "send-deploy.sh"

SHA = "0123456789abcdef0123456789abcdef01234567"
OTHER = "89abcdef0123456789abcdef0123456789abcdef"
INSTANCE = "i-0123456789abcdef0"
COMMAND_ID = "0a1b2c3d-4e5f-6789-abcd-ef0123456789"
NOW = 1791234000
SENTINEL = "SENTINEL-7f3a9c"
ACCOUNT = "123456789012"

UNKNOWN = (
    "outcome unknown: compare HEAD with /var/lib/marketlake/deployed on the VM, "
    "and read /var/lib/marketlake/deploy.log"
)

FAKE_AWS = (
    "#!/bin/bash\n"
    'printf \'aws %s\\n\' "$*" >> "$LOG"\n'
    + NEXT_RC
    + r"""query=""
prev=""
for arg in "$@"; do
  if [[ "$prev" == --query ]]; then query="$arg"; fi
  prev="$arg"
done
fail() {
  printf '\nAn error occurred (%s) when calling the %s operation: ' "$1" "$2" >&2
  printf 'User: arn:aws:sts::%s:assumed-role/marketlake-deploy/x is not authorized %s\n' \
    "$FAKE_ACCOUNT" "$SENTINEL" >&2
  exit 254
}
case "$1 $2" in
  "ec2 describe-instances")
    if [[ -n "${DESCRIBE_ERROR:-}" ]]; then fail "$DESCRIBE_ERROR" DescribeInstances; fi
    printf '%s\n' "$INSTANCES"
    ;;
  "ssm send-command")
    if [[ -n "${SEND_ERROR:-}" ]]; then fail "$SEND_ERROR" SendCommand; fi
    printf '%s\n' "$FAKE_COMMAND_ID"
    ;;
  "ssm get-command-invocation")
    if [[ "$query" == StandardOutputContent ]]; then
      if [[ -n "${OUTPUT_ERROR:-}" ]]; then fail "$OUTPUT_ERROR" GetCommandInvocation; fi
      cat "$HOST_OUTPUT"
      echo
      exit 0
    fi
    answer="$(next_rc invocation "$INVOCATIONS")"
    case "$answer" in
      DNE) fail InvocationDoesNotExist GetCommandInvocation ;;
      ERR) fail ThrottlingException GetCommandInvocation ;;
      *) printf '%s\t%s\n' "${answer%%:*}" "${answer#*:}" ;;
    esac
    ;;
  *)
    echo "fake aws: unexpected $*" >&2
    exit 99
    ;;
esac
"""
)

FAKE_DATE = (
    "#!/bin/bash\n"
    + NEXT_RC
    + r"""if [[ "$*" != +%s ]]; then echo "fake date: unexpected $*" >&2; exit 9; fi
next_rc date "$DATES"
"""
)


@pytest.fixture(scope="module")
def bin_dir(tmp_path_factory) -> Path:
    directory = tmp_path_factory.mktemp("send-deploy-tools") / "bin"
    install(directory / "aws", FAKE_AWS)
    install(directory / "date", FAKE_DATE)
    install(directory / "sleep", FAKE_SLEEP)
    return directory


class Runner:
    def __init__(self, tmp_path: Path, bin_dir: Path) -> None:
        self.log = tmp_path / "log"
        self.state = tmp_path / "state"
        self.summary = tmp_path / "summary"
        self.output = tmp_path / "host-output"
        self.tmp = tmp_path / "tmp"
        for directory in (self.state, self.tmp):
            directory.mkdir()
        self.log.write_text("")
        self.summary.write_text("")
        self.host_says(f"deployed: {SHA}")
        self.env = {
            "PATH": f"{bin_dir}:/usr/bin:/bin",
            "TMPDIR": str(self.tmp),
            "LOG": str(self.log),
            "STATE": str(self.state),
            "GITHUB_SHA": SHA,
            "GITHUB_STEP_SUMMARY": str(self.summary),
            "DEPLOY_DOCUMENT": "marketlake-deploy",
            "DEPLOY_TAG_KEY": "marketlake:host",
            "DEPLOY_TAG_VALUE": "capture",
            "INSTANCES": INSTANCE,
            "FAKE_COMMAND_ID": COMMAND_ID,
            "FAKE_ACCOUNT": ACCOUNT,
            "HOST_OUTPUT": str(self.output),
            "INVOCATIONS": "Success:0",
            "DATES": str(NOW),
            "SENTINEL": SENTINEL,
        }

    def host_says(self, last: str, newline: str = "\n") -> None:
        """The host's full output: progress lines carrying the sentinel, then ``last``."""
        lines = [f"vm-deploy: step one {SENTINEL}", f"config value {SENTINEL}", last, ""]
        self.output.write_text(newline.join(lines) + newline)

    def run(self, **env: str) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            ["/bin/bash", str(SEND)],
            env={**self.env, **env},
            cwd=self.tmp,
            capture_output=True,
            text=True,
            timeout=60,
        )
        # Whatever happened, nothing AWS handed back reaches a surface anyone can read.
        for surface in (result.stdout, result.stderr, self.summary.read_text()):
            assert SENTINEL not in surface, surface
            assert ACCOUNT not in surface, surface
            assert "{" not in surface, surface
        # The scratch directory goes with the run.
        assert list(self.tmp.iterdir()) == []
        return result

    def calls(self, prefix: str = "") -> list[str]:
        return [line for line in self.log.read_text().splitlines() if line.startswith(prefix)]


@pytest.fixture
def runner(tmp_path, bin_dir) -> Runner:
    return Runner(tmp_path, bin_dir)


def _summary(result: subprocess.CompletedProcess[str]) -> str:
    lines = [line for line in result.stdout.splitlines() if line.startswith("send-deploy: ")]
    return lines[-2].removeprefix("send-deploy: ")


def _assert_reported(
    runner: Runner,
    result: subprocess.CompletedProcess[str],
    rc: int,
    summary: str,
    status: str,
    code: str,
) -> None:
    assert result.returncode == rc, result.stdout + result.stderr
    assert result.stderr == ""
    assert result.stdout.splitlines()[-2:] == [
        f"send-deploy: {summary}",
        f"send-deploy: StatusDetails {status}, ResponseCode {code}",
    ], result.stdout
    assert runner.summary.read_text() == (
        f"### Deploy of {SHA}\n\n{summary}\n\nStatusDetails {status}, ResponseCode {code}\n"
    )


# -- the table -------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("invocation", "last", "rc"),
    [
        ("Success:0", f"deployed: {SHA}", 0),
        ("Success:0", f"deployed: {SHA} (already current)", 0),
        ("Failed:1", f"deployed: {SHA}, with a failed step in deploy.log", 1),
        ("Failed:1", f"deployed: {SHA}, but the dashboard did not restart", 1),
        (
            "Failed:1",
            f"deployed: {SHA}, with a failed step in deploy.log, but the dashboard did not restart",
            1,
        ),
        (
            "Failed:1",
            f"rolled back to {OTHER}: the daemon did not hold, which has not run before",
            1,
        ),
        ("Failed:1", f"rollback to {OTHER} failed: the bootstrap exited 1", 1),
        (
            "Failed:1",
            f"not restarted: the tree is at {SHA}, and the last recorded deploy is {OTHER}",
            1,
        ),
        (
            "Failed:1",
            f"not restarted: the tree is at {SHA}, and the last recorded deploy is none",
            1,
        ),
        ("Failed:1", "not deployed: the fetch failed", 1),
        ("Failed:2", "not deployed: the daemon is not active", 2),
        ("Failed:3", "not deployed: a deploy may start at 18:45 ET", 3),
        ("Failed:1", "outcome unknown: read deploy.log", 1),
    ],
)
def test_a_host_line_is_the_summary_and_its_code_the_exit(runner, invocation, last, rc):
    runner.host_says(last)
    result = runner.run(INVOCATIONS=invocation)
    status, code = invocation.split(":")
    _assert_reported(runner, result, rc, last, status, code)


def test_a_host_line_ending_in_a_carriage_return_still_counts(runner):
    runner.host_says(f"deployed: {SHA}", newline="\r\n")
    result = runner.run()
    _assert_reported(runner, result, 0, f"deployed: {SHA}", "Success", "0")


def test_a_missing_vm_deploy_asks_for_the_manual_first_deploy(runner):
    runner.output.write_text("")
    result = runner.run(INVOCATIONS="Failed:127")
    _assert_reported(
        runner,
        result,
        1,
        "vm-deploy.sh is missing on the VM, so run the manual first deploy",
        "Failed",
        "127",
    )


@pytest.mark.parametrize(
    "status",
    ["DeliveryTimedOut", "Undeliverable", "Terminated", "AccessDenied", "InvalidPlatform"],
)
def test_a_command_never_delivered_asks_for_a_re_run(runner, status):
    runner.output.write_text("")
    result = runner.run(INVOCATIONS=f"{status}:-1")
    _assert_reported(runner, result, 1, "not delivered, so re-run", status, "-1")


def test_a_command_cancelled_before_it_started_is_not_started(runner):
    runner.output.write_text("")
    result = runner.run(INVOCATIONS="Cancelled:-1")
    _assert_reported(runner, result, 1, "not started", "Cancelled", "-1")


@pytest.mark.parametrize(
    ("invocation", "last"),
    [
        # The agent's own kill, and a cancel after the start, leave no host line.
        ("ExecutionTimedOut:137", ""),
        ("ExecutionTimedOut:1", ""),
        ("Cancelled:1", ""),
        ("Failed:1", ""),
        ("Failed:137", ""),
        # A code with a host line is still unknown when the code is not the host's.
        ("Failed:137", f"deployed: {SHA}"),
        ("Failed:4", "not deployed: the daemon is not active"),
        # Success counts only with a host line.
        ("Success:0", ""),
        ("Success:0", "something the host's table does not hold"),
        # A line that only resembles a row.
        ("Failed:2", "not deployed:"),
        ("Failed:1", f"deployed: {SHA[:39]}"),
        ("Failed:1", f"deployed: {SHA} and more"),
        ("Failed:1", f"note: deployed: {SHA}"),
        ("Failed:1", "outcome unknown: something else"),
        ("Failed:1", f"not restarted: the tree is at {SHA}, and the last recorded deploy is"),
        ("Failed:2", "not deployed: "),
        # A status the table does not list, or a listed one with another code.
        ("Weird:0", f"deployed: {SHA}"),
        ("Undeliverable:0", ""),
        ("Cancelled:0", ""),
        ("Failed:-1", ""),
        ("Cancelled:127", ""),
        ("ExecutionTimedOut:127", ""),
    ],
)
def test_anything_else_is_outcome_unknown(runner, invocation, last):
    runner.host_says(last)
    result = runner.run(INVOCATIONS=invocation)
    status, code = invocation.split(":")
    _assert_reported(runner, result, 1, UNKNOWN, status, code)


def test_a_reply_that_is_neither_a_status_nor_a_code_prints_neither(runner):
    result = runner.run(INVOCATIONS=f"{SENTINEL}:{SENTINEL}")
    _assert_reported(runner, result, 1, UNKNOWN, "unreadable", "unreadable")


def test_an_unreadable_output_falls_to_the_rows_without_a_host_line(runner):
    result = runner.run(INVOCATIONS="Success:0", OUTPUT_ERROR="ThrottlingException")
    _assert_reported(runner, result, 1, UNKNOWN, "Success", "0")
    result = runner.run(INVOCATIONS="Cancelled:-1", OUTPUT_ERROR="ThrottlingException")
    assert _summary(result) == "not started"


# -- the wait --------------------------------------------------------------------------


def test_the_wait_retries_a_missing_invocation_then_reads_the_statuses(runner):
    result = runner.run(INVOCATIONS="DNE DNE Pending:-1 InProgress:-1 Delayed:-1 Success:0")
    _assert_reported(runner, result, 0, f"deployed: {SHA}", "Success", "0")
    assert runner.calls("sleep") == ["sleep 15"] * 5
    status_reads = [c for c in runner.calls("aws ssm get-command-invocation") if "Status" in c]
    assert len(status_reads) == 6


def test_cancelling_is_waited_through(runner):
    runner.output.write_text("")
    result = runner.run(INVOCATIONS="InProgress:-1 Cancelling:-1 Cancelled:1")
    _assert_reported(runner, result, 1, UNKNOWN, "Cancelled", "1")
    assert runner.calls("sleep") == ["sleep 15"] * 2


def test_a_missing_invocation_past_not_after_is_outcome_unknown(runner):
    """The first date is the send time, so notAfter is NOW + 600, and the retries stop
    at the first read that is not before it."""
    dates = f"{NOW} {NOW + 1} {NOW + 599} {NOW + 600}"
    result = runner.run(INVOCATIONS="DNE", DATES=dates)
    _assert_reported(
        runner,
        result,
        1,
        f"{UNKNOWN}. GetCommandInvocation failed with InvocationDoesNotExist",
        "none",
        "none",
    )
    assert runner.calls("sleep") == ["sleep 15"] * 2


def test_any_other_polling_error_is_outcome_unknown(runner):
    result = runner.run(INVOCATIONS="InProgress:-1 ERR")
    _assert_reported(
        runner,
        result,
        1,
        f"{UNKNOWN}. GetCommandInvocation failed with ThrottlingException",
        "InProgress",
        "-1",
    )


# -- the send --------------------------------------------------------------------------


def test_the_send_names_the_instance_the_document_and_the_window(runner):
    result = runner.run()
    assert result.returncode == 0, result.stdout + result.stderr
    calls = runner.calls("aws")
    assert calls[0] == (
        "aws ec2 describe-instances --filters Name=tag:marketlake:host,Values=capture "
        "Name=instance-state-name,Values=running "
        "--query Reservations[].Instances[].InstanceId --output text"
    )
    assert calls[1] == (
        "aws ssm send-command --document-name marketlake-deploy "
        f"--instance-ids {INSTANCE} "
        f'--parameters {{"sha":["{SHA}"],"notAfter":["{NOW + 600}"]}} '
        "--timeout-seconds 600 --query Command.CommandId --output text"
    )
    assert "--targets" not in calls[1]
    for call in calls[2:]:
        assert call.startswith(
            f"aws ssm get-command-invocation --command-id {COMMAND_ID} --instance-id {INSTANCE} "
        ), call
    assert (
        result.stdout.splitlines()[0]
        == f"send-deploy: sent {SHA} to the VM, and waiting for it to end"
    )


@pytest.mark.parametrize(
    ("instances", "count"),
    [("", 0), (f"{INSTANCE}\ti-0fedcba9876543210", 2), (f"{INSTANCE}\ni-0fedcba9876543210", 2)],
    ids=["none", "two-tab", "two-line"],
)
def test_anything_but_one_instance_sends_nothing(runner, instances, count):
    result = runner.run(INSTANCES=instances)
    _assert_reported(
        runner,
        result,
        1,
        f"not sent: {count} running instances carry the tag marketlake:host = capture, "
        "and a deploy needs exactly one",
        "none",
        "none",
    )
    assert not runner.calls("aws ssm")


def test_an_id_that_is_not_an_instance_sends_nothing(runner):
    result = runner.run(INSTANCES="None")
    _assert_reported(
        runner,
        result,
        1,
        "not sent: DescribeInstances returned something that is not an instance id",
        "none",
        "none",
    )
    assert not runner.calls("aws ssm")


def test_a_failed_lookup_sends_nothing_and_prints_only_the_code(runner):
    result = runner.run(DESCRIBE_ERROR="UnauthorizedOperation")
    _assert_reported(
        runner,
        result,
        1,
        "not sent: DescribeInstances failed with UnauthorizedOperation",
        "none",
        "none",
    )
    assert not runner.calls("aws ssm")


def test_a_failed_send_is_not_sent_and_prints_only_the_code(runner):
    result = runner.run(SEND_ERROR="AccessDeniedException")
    _assert_reported(
        runner, result, 1, "not sent: SendCommand failed with AccessDeniedException", "none", "none"
    )
    assert not runner.calls("aws ssm get-command-invocation")


def test_a_send_with_no_readable_id_is_outcome_unknown(runner):
    result = runner.run(FAKE_COMMAND_ID=SENTINEL)
    _assert_reported(
        runner,
        result,
        1,
        f"{UNKNOWN}. SendCommand returned no readable command id",
        "none",
        "none",
    )


# -- the inputs ------------------------------------------------------------------------


@pytest.mark.parametrize("sha", ["", SHA[:39], SHA.upper(), SHA + "0", f"{SHA[:20]} {SHA[21:]}"])
def test_a_sha_that_is_not_a_commit_sends_nothing(runner, sha):
    result = runner.run(GITHUB_SHA=sha)
    assert result.returncode == 1
    assert _summary(result) == "not sent: GITHUB_SHA is not a 40-digit commit"
    assert runner.calls("aws") == []


@pytest.mark.parametrize("name", ["DEPLOY_DOCUMENT", "DEPLOY_TAG_KEY", "DEPLOY_TAG_VALUE"])
def test_an_empty_setting_sends_nothing(runner, name):
    result = runner.run(**{name: ""})
    _assert_reported(runner, result, 1, f"not sent: {name} is empty", "none", "none")
    assert runner.calls("aws") == []


def test_no_summary_file_still_prints_the_summary(runner):
    env = dict(runner.env)
    del env["GITHUB_STEP_SUMMARY"]
    result = subprocess.run(
        ["/bin/bash", str(SEND)], env=env, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert re.search(rf"^send-deploy: deployed: {SHA}$", result.stdout, re.MULTILINE)
