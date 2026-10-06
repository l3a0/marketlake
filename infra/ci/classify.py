"""Decide whether a merged infra change may apply with no click.

The merge of a reviewed pull request is the owner's approval, so a plan on ``main`` that
does only what the pull request's own plan showed may apply without a second click.
Anything else waits for the owner in the ``infra`` environment. ``docs/design.md``'s
"Infrastructure, defined" carries the reasoning, and its considered-and-rejected
register says why the rules are these and not a market clock or a list of dangerous
types. The script has three modes, one for each step of the infra workflow that runs it.

1. ``change-set`` runs on the pull request. It reads ``tofu show -json`` on stdin and
   writes the plan's change set to ``--out``, for the workflow to upload as an artifact.
   The change set holds one row per resource the plan changes or imports: the address
   with its keys replaced by ``[…]``, the raw actions list, an import flag and the names
   of the changed attributes. It never holds a value, because anyone signed in can
   download a public repository's artifacts.
2. ``fetch`` runs on ``main``. It finds the pull request the pushed commit merged,
   checks that the artifact named for that pull request's head came from this
   repository's own ``pull_request`` run of the workflow, and writes the reviewed change
   set to ``--out``. Any failure leaves no file, prints the reason and exits 0, so the
   lookup never fails the job.
3. ``decide`` runs on ``main`` after ``fetch``. It reads ``tofu show -json`` of the plan
   the job is about to apply, checks the four rules, writes ``verdict=apply`` or
   ``verdict=refuse`` to ``$GITHUB_OUTPUT``, and exits 0 for either. It exits 1 only when
   this run's own input is malformed, which fails the job and so reaches the owner.

The four rules range over the change set's rows, never over the ``no-op`` entry a plan
lists for every unchanged resource.

1. The change set is empty, or it equals the reviewed one on a first ``push`` run.
2. Every action is ``no-op``, ``create``, ``update`` or ``read``.
3. No changed type starts with ``aws_iam_`` or ``aws_kms_``, no changed ``bucket`` is the
   state bucket or unknown, and no changed attribute's name contains ``policy``.
4. Every changed type is on ``ALLOWLIST``.

Each mode prints Markdown for the step summary, built only from rule names, actions,
resource types and redacted addresses. Standard library only, so the workflow runs it
with the runner's own ``python3``.

Usage::

    tofu show -json plan.tfplan | python3 infra/ci/classify.py change-set --out FILE
    python3 infra/ci/classify.py fetch --name-prefix PREFIX --member NAME --out FILE
    tofu show -json plan.tfplan | python3 infra/ci/classify.py decide --reviewed FILE
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import zipfile
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, NamedTuple, TextIO

# Run as a script, Python already puts this directory on the path. A test that loads the
# file by path needs it added.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from plan_summary import _has_unknown, address_without_keys, changed_attributes  # noqa: E402

WORKFLOW_PATH = ".github/workflows/infra.yml"

# Anything else refuses: delete, both replace orders, forget, and any action a later
# OpenTofu adds.
SAFE_ACTIONS = frozenset({"no-op", "create", "update", "read"})

UNSAFE_TYPE_PREFIXES = ("aws_iam_", "aws_kms_")

# A type belongs here only when no change to it can interrupt capture or reach the VM.
# What a change does to data or access is in the diff, and the merge reviews it.
# Adding a type is a reviewed one-line change that names why it meets that bar.
# aws_s3_bucket and aws_s3_bucket_versioning stay off, because the apply role cannot
# write them.
ALLOWLIST = frozenset(
    {
        "aws_s3_bucket_server_side_encryption_configuration",
        "aws_s3_bucket_public_access_block",
        "aws_s3_bucket_lifecycle_configuration",
    }
)

_ROW_FIELDS: dict[str, type] = {
    "address": str,
    "actions": list,
    "import": bool,
    "attributes": list,
}

ChangeSet = list[dict[str, Any]]
Api = Callable[[str], bytes]


class Rule(NamedTuple):
    number: int
    name: str
    passed: bool
    detail: str


# -- the change set -----------------------------------------------------------------


def _is_row(resource: Mapping[str, Any]) -> bool:
    """Whether ``plan_summary.rows()`` prints the resource. A ``no-op`` import is a row."""
    change = resource["change"]
    return change["actions"] != ["no-op"] or change.get("importing") is not None


def plan_rows(plan: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """The plan's entries for resources it changes or imports, in the plan's order."""
    return [resource for resource in plan.get("resource_changes") or [] if _is_row(resource)]


def _row_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    # Python cannot order two dicts, so rows sort by their fields.
    return (row["address"], row["actions"], row["import"], row["attributes"])


def change_set(plan: Mapping[str, Any]) -> ChangeSet:
    """The plan's change set: sorted, duplicates kept, lists and dicts only.

    JSON reads a tuple back as a list, and a tuple never equals a list, so a set built
    with tuples would never match the reviewed set read from a file.
    """
    out = []
    for resource in plan_rows(plan):
        change = resource["change"]
        actions = list(change["actions"])
        names = [] if actions in (["create"], ["delete"]) else changed_attributes(change)
        out.append(
            {
                "address": address_without_keys(resource["address"]),
                "actions": actions,
                "import": change.get("importing") is not None,
                "attributes": list(names),
            }
        )
    return sorted(out, key=_row_key)


def encode(rows: ChangeSet) -> bytes:
    """The one function that writes a change set's bytes, on both sides of the merge."""
    return (json.dumps(rows, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def load_change_set(data: bytes) -> ChangeSet:
    """A change set read back from bytes, refused unless every row has the right shape."""
    rows = json.loads(data)
    if not isinstance(rows, list):
        raise ValueError("not a list")
    for row in rows:
        if not isinstance(row, dict) or set(row) != set(_ROW_FIELDS):
            raise ValueError("a row has the wrong fields")
        for field, kind in _ROW_FIELDS.items():
            if not isinstance(row[field], kind):
                raise ValueError("a field has the wrong type")
        if not all(isinstance(word, str) for word in row["actions"] + row["attributes"]):
            raise ValueError("a list holds something other than a string")
    return rows


def read_member(blob: bytes, member: str) -> ChangeSet:
    """The change set inside an artifact's zip, read in memory by member name."""
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        return load_change_set(archive.read(member))


# -- fetch mode ---------------------------------------------------------------------


def runner_int(value: str | None) -> int | None:
    """The one place a runner variable becomes an integer, or ``None`` if it does not parse.

    The runner's ``GITHUB_REPOSITORY_ID`` and ``GITHUB_RUN_ATTEMPT`` are strings, while
    the API's ids are integers. Compared raw, the id never matches.
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def pick_pull_request(
    pulls: Sequence[Mapping[str, Any]], sha: str
) -> tuple[Mapping[str, Any] | None, str]:
    """The one pull request that merged as ``sha``, or ``None`` and the reason."""
    merged = [pull for pull in pulls if pull.get("merge_commit_sha") == sha]
    if not merged:
        return None, "no pull request merged as this commit"
    if len(merged) > 1:
        return None, "more than one pull request merged as this commit"
    return merged[0], ""


def run_is_reviewed(run: Mapping[str, Any], head_sha: str, repository_id: int) -> bool:
    """Whether a run is this repository's own ``pull_request`` run of the workflow at the head.

    The ids sit at ``head_repository.id``. The run's top-level ``head_repository_id`` is
    null, so reading it would compare ``None`` with ``None`` and accept a fork. A missing
    or null id never equals ``repository_id``, which is always an integer here.
    """
    return (
        run.get("path") == WORKFLOW_PATH
        and run.get("event") == "pull_request"
        and run.get("head_sha") == head_sha
        and (run.get("head_repository") or {}).get("id") == repository_id
    )


def pick_artifact(
    artifacts: Sequence[Mapping[str, Any]],
    runs: Mapping[int, Mapping[str, Any]],
    head_sha: str,
    repository_id: int,
) -> Mapping[str, Any] | None:
    """The newest unexpired artifact whose run passes ``run_is_reviewed``.

    The origin check comes before the pick, so a fork's newer artifact under the same
    name cannot crowd out the reviewed one.
    """
    reviewed = [
        artifact
        for artifact in artifacts
        if artifact.get("expired") is False
        and run_is_reviewed(runs.get(artifact["workflow_run"]["id"], {}), head_sha, repository_id)
    ]
    if not reviewed:
        return None
    return max(reviewed, key=lambda artifact: artifact["created_at"])


def _gh_api(path: str) -> bytes:
    return subprocess.run(["gh", "api", path], capture_output=True, check=True).stdout


def _describe(error: Exception) -> str:
    """The error's type, and the HTTP status ``gh`` reported, never its message."""
    text = type(error).__name__
    if isinstance(error, subprocess.CalledProcessError):
        status = re.search(rb"HTTP (\d{3})", error.stderr or b"")
        if status:
            text += f" (HTTP {status.group(1).decode()})"
    return text


def _lookup(
    api: Api,
    *,
    repository: str,
    sha: str,
    repository_id: int,
    prefix: str,
    member: str,
    out: Path,
) -> str:
    pull, why = pick_pull_request(json.loads(api(f"repos/{repository}/commits/{sha}/pulls")), sha)
    if pull is None:
        return f"not found, because {why}."
    head_sha = pull["head"]["sha"]
    listing = json.loads(
        api(f"repos/{repository}/actions/artifacts?name={prefix}{head_sha}&per_page=100")
    )
    artifacts = listing["artifacts"]
    if not artifacts:
        return "not found, because no artifact is named for the pull request's head."
    runs = {}
    for artifact in artifacts:
        run_id = artifact["workflow_run"]["id"]
        if artifact.get("expired") is False and run_id not in runs:
            runs[run_id] = json.loads(api(f"repos/{repository}/actions/runs/{run_id}"))
    artifact = pick_artifact(artifacts, runs, head_sha, repository_id)
    if artifact is None:
        return (
            "not found, because no unexpired artifact came from this repository's own"
            " pull request run of the workflow at the head."
        )
    blob = api(f"repos/{repository}/actions/artifacts/{artifact['id']}/zip")
    reviewed = read_member(blob, member)
    data = encode(reviewed)
    out.write_bytes(data)
    return f"found, from pull request #{int(pull['number'])}, sha256 `{sha256(data)}`."


def fetch(
    api: Api,
    *,
    repository: str,
    sha: str,
    repository_id: str | None,
    prefix: str,
    member: str,
    out: Path,
) -> str:
    """Write the reviewed change set to ``out`` and return the summary's reason.

    Every failure writes no file and names its reason, so decide mode refuses under
    rule 1 and the owner reads why.
    """
    number = runner_int(repository_id)
    if number is None:
        return "not found, because GITHUB_REPOSITORY_ID is not a number."
    try:
        return _lookup(
            api,
            repository=repository,
            sha=sha,
            repository_id=number,
            prefix=prefix,
            member=member,
            out=out,
        )
    except Exception as error:
        return f"not found, because the lookup failed with {_describe(error)}."


# -- decide mode --------------------------------------------------------------------


def _touches_state_bucket(change: Mapping[str, Any], state_bucket: str) -> bool:
    before = change.get("before") or {}
    after = change.get("after") or {}
    unknown = change.get("after_unknown") or {}
    return (
        before.get("bucket") == state_bucket
        or after.get("bucket") == state_bucket
        or _has_unknown(unknown.get("bucket"))
    )


def _breaks_floor(resource: Mapping[str, Any], state_bucket: str) -> bool:
    """Whether a row fails rule 3, the floor no allowlist entry can lift.

    The attribute names come from the plan entry, not from the change set, which lists
    none for a create. For a create, ``changed_attributes`` returns every name non-null
    in ``after`` or marked in ``after_unknown``, so a new ``aws_s3_bucket_policy`` and a
    policy built from a not-yet-known ARN both refuse.
    """
    change = resource["change"]
    return (
        resource["type"].startswith(UNSAFE_TYPE_PREFIXES)
        or _touches_state_bucket(change, state_bucket)
        or any("policy" in name for name in changed_attributes(change))
    )


def _addresses(resources: Sequence[Mapping[str, Any]]) -> str:
    return ", ".join(f"`{address_without_keys(resource['address'])}`" for resource in resources)


def _rule(number: int, name: str, failing: Sequence[Mapping[str, Any]]) -> Rule:
    if failing:
        return Rule(number, name, False, "refused by " + _addresses(failing))
    return Rule(number, name, True, "")


def rules(
    plan: Mapping[str, Any],
    reviewed: ChangeSet | None,
    *,
    state_bucket: str,
    event: str | None,
    attempt: int | None,
) -> list[Rule]:
    """The four rules' results, in order."""
    current = change_set(plan)
    name = "the change set is empty or the reviewed one"
    if not current:
        first = Rule(1, name, True, "the plan changes nothing")
    elif event != "push":
        first = Rule(1, name, False, "only a push has a reviewed change set")
    elif attempt != 1:
        first = Rule(1, name, False, "a re-run is never approved by the merge")
    elif reviewed is None:
        first = Rule(1, name, False, "no reviewed change set was found")
    elif current != reviewed:
        first = Rule(1, name, False, "the change set differs from the reviewed one")
    else:
        first = Rule(1, name, True, "the change set matches the reviewed one")

    rows = plan_rows(plan)
    return [
        first,
        _rule(
            2,
            "nothing is destroyed or replaced",
            [r for r in rows if any(a not in SAFE_ACTIONS for a in r["change"]["actions"])],
        ),
        _rule(
            3,
            "no IAM, KMS, state bucket or policy change",
            [r for r in rows if _breaks_floor(r, state_bucket)],
        ),
        _rule(
            4,
            "every changed type is on the allowlist",
            [r for r in rows if r["type"] not in ALLOWLIST],
        ),
    ]


def _read_reviewed(path: Path) -> tuple[ChangeSet | None, str]:
    try:
        return load_change_set(path.read_bytes()), "found"
    except FileNotFoundError:
        return None, "not found"
    except Exception:
        return None, "unreadable"


def decide(stdin: TextIO, reviewed_path: Path, environ: Mapping[str, str]) -> tuple[str, list[str]]:
    """The verdict and the summary's lines. Raises on this run's own malformed input."""
    plan = json.load(stdin)
    state_bucket = environ["TF_STATE_BUCKET"]
    if not state_bucket:
        raise ValueError("TF_STATE_BUCKET is empty")
    reviewed, found = _read_reviewed(reviewed_path)
    current = change_set(plan)
    results = rules(
        plan,
        reviewed,
        state_bucket=state_bucket,
        event=environ.get("GITHUB_EVENT_NAME"),
        attempt=runner_int(environ.get("GITHUB_RUN_ATTEMPT")),
    )
    verdict = "apply" if all(rule.passed for rule in results) else "refuse"
    if reviewed is not None:
        found += ", matched" if current == reviewed else ", differs"
    lines = [
        f"Reviewed change set: {found}.",
        "",
        f"This plan's change set: {len(current)} rows, sha256 `{sha256(encode(current))}`.",
        "",
    ]
    for rule in results:
        result = "pass" if rule.passed else "refuse"
        detail = f", {rule.detail}" if rule.detail else ""
        lines.append(f"- Rule {rule.number}, {rule.name}: {result}{detail}.")
    lines.append("")
    if verdict == "apply":
        lines.append("Verdict: apply with no click.")
    else:
        lines.append("Verdict: refuse. The `apply` job waits for the owner's approval in `infra`.")
    return verdict, lines


# -- the command line ---------------------------------------------------------------


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="classify.py")
    modes = parser.add_subparsers(dest="mode", required=True)
    change = modes.add_parser("change-set")
    change.add_argument("--out", type=Path, required=True)
    lookup = modes.add_parser("fetch")
    lookup.add_argument("--name-prefix", required=True)
    lookup.add_argument("--member", required=True)
    lookup.add_argument("--out", type=Path, required=True)
    verdict = modes.add_parser("decide")
    verdict.add_argument("--reviewed", type=Path, required=True)
    return parser


def _write(stdout: TextIO, title: str, lines: Sequence[str]) -> None:
    # The blank line and heading end any table the step summary already holds.
    stdout.write("\n".join(["", f"### {title}", "", *lines]) + "\n")


def main(
    argv: list[str],
    stdin: TextIO,
    stdout: TextIO,
    *,
    environ: Mapping[str, str] | None = None,
    api: Api = _gh_api,
) -> int:
    args = _parser().parse_args(argv[1:])
    env = os.environ if environ is None else environ
    if args.mode == "change-set":
        rows = change_set(json.load(stdin))
        data = encode(rows)
        args.out.write_bytes(data)
        _write(stdout, "Change set", [f"{len(rows)} rows, sha256 `{sha256(data)}`."])
        return 0
    if args.mode == "fetch":
        reason = fetch(
            api,
            repository=env.get("GITHUB_REPOSITORY", ""),
            sha=env.get("GITHUB_SHA", ""),
            repository_id=env.get("GITHUB_REPOSITORY_ID"),
            prefix=args.name_prefix,
            member=args.member,
            out=args.out,
        )
        _write(stdout, "Reviewed change set lookup", [reason])
        return 0
    try:
        verdict, lines = decide(stdin, args.reviewed, env)
        with open(env["GITHUB_OUTPUT"], "a") as output:
            output.write(f"verdict={verdict}\n")
    except Exception as error:
        # A KeyError or ValueError quotes its input, which can be a bucket name.
        _write(stdout, "No-click apply", [f"malformed input: {type(error).__name__}"])
        return 1
    _write(stdout, "No-click apply", lines)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv, sys.stdin, sys.stdout))
