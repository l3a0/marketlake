"""``infra/ci/classify.py`` decides which merged plans apply with no click.

A wrong accept can destroy the lake's volume or stop capture, so every rule gets a
fixture that it alone refuses. Each refused case passes the other three rules where it
can: the reviewed set matches, the run is a ``push`` at attempt 1, and the type is on
the allowlist. Each decide test reads the verdict from ``$GITHUB_OUTPUT`` and the
refused rules from the step summary, so a rule that another rule masks is still caught.

The lookup's fixtures are GitHub's own JSON, trimmed. ``PULLS`` is
``GET repos/l3a0/marketlake/commits/bb1cecc/pulls``, ``RUN`` is
``GET repos/l3a0/marketlake/actions/runs/37522642949``, that pull request's own run,
and ``NO_ARTIFACT`` is an artifact listing for a name nothing uploaded. No change set
artifact exists yet, so ``ARTIFACT`` is a real artifact from the listing with its name
and ``workflow_run`` set to this scenario. Every plan value is a marker string, and
each test checks that no marker reaches a summary.
"""

from __future__ import annotations

import copy
import importlib.util
import io
import json
import os
import re
import stat
import subprocess
import sys
import zipfile
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "infra" / "ci" / "classify.py"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("classify", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


classify = _load()

STATE_BUCKET = "VALUE-STATE-BUCKET"
BACKUP_BUCKET = "VALUE-BACKUP-BUCKET"
MARKER = "VALUE-"

REPOSITORY = "l3a0/marketlake"
REPOSITORY_ID = "1346754080"
MERGE_SHA = "bb1cecc6178cc8f278a89b9dac13c6da42988238"
HEAD_SHA = "3d43711e329660e83c3241c4c68a6ba9674fa03c"
PREFIX = "infra-change-set-"
MEMBER = "change-set.json"

PULLS: list[dict[str, Any]] = [
    {
        "url": "https://api.github.com/repos/l3a0/marketlake/pulls/710",
        "number": 710,
        "state": "closed",
        "title": "docs(infra): add a standalone bootstrap runbook in infra/README.md",
        "merged_at": "2026-10-06T20:24:56Z",
        "merge_commit_sha": "bb1cecc6178cc8f278a89b9dac13c6da42988238",
        "head": {
            "ref": "claude/infra-runbook-664",
            "sha": "3d43711e329660e83c3241c4c68a6ba9674fa03c",
            "repo": {"id": 1346754080, "full_name": "l3a0/marketlake"},
        },
        "base": {"ref": "main", "repo": {"id": 1346754080, "full_name": "l3a0/marketlake"}},
    }
]

RUN: dict[str, Any] = {
    "id": 37522642949,
    "name": "Infra",
    "head_branch": "claude/infra-runbook-664",
    "head_sha": "3d43711e329660e83c3241c4c68a6ba9674fa03c",
    "path": ".github/workflows/infra.yml",
    "run_number": 8,
    "event": "pull_request",
    "status": "completed",
    "conclusion": "success",
    "workflow_id": 376135489,
    "run_attempt": 1,
    "repository": {"id": 1346754080, "name": "marketlake", "full_name": "l3a0/marketlake"},
    "head_repository": {"id": 1346754080, "name": "marketlake", "full_name": "l3a0/marketlake"},
}

NO_ARTIFACT: dict[str, Any] = {"total_count": 0, "artifacts": []}

ARTIFACT: dict[str, Any] = {
    "id": 11451201449,
    "node_id": "MDg6QXJ0aWZhY3QxMTQ1MTIwMTQ0OQ==",
    "name": PREFIX + HEAD_SHA,
    "size_in_bytes": 19807,
    "url": "https://api.github.com/repos/l3a0/marketlake/actions/artifacts/11451201449",
    "archive_download_url": (
        "https://api.github.com/repos/l3a0/marketlake/actions/artifacts/11451201449/zip"
    ),
    "expired": False,
    "digest": "sha256:3015d7d16fefea4ee4ce5498466783564233ceb641f4759865be9fff7da577fb",
    "created_at": "2026-10-06T23:25:17Z",
    "updated_at": "2026-10-06T23:25:17Z",
    "expires_at": "2027-01-04T23:24:39Z",
    "workflow_run": {
        "id": 37522642949,
        "repository_id": 1346754080,
        "head_repository_id": 1346754080,
        "head_branch": "claude/infra-runbook-664",
        "head_sha": "3d43711e329660e83c3241c4c68a6ba9674fa03c",
    },
}

PULLS_PATH = f"repos/{REPOSITORY}/commits/{MERGE_SHA}/pulls"
LISTING_PATH = f"repos/{REPOSITORY}/actions/artifacts?name={PREFIX}{HEAD_SHA}&per_page=100"


def _run_path(run_id: int) -> str:
    return f"repos/{REPOSITORY}/actions/runs/{run_id}"


def _zip_path(artifact_id: int) -> str:
    return f"repos/{REPOSITORY}/actions/artifacts/{artifact_id}/zip"


# -- plans --------------------------------------------------------------------------


def _entry(
    address: str,
    kind: str,
    actions: list[str],
    before: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
    unknown: dict[str, Any] | None = None,
    *,
    mode: str = "managed",
    importing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    change: dict[str, Any] = {
        "actions": actions,
        "before": before,
        "after": after,
        "after_unknown": unknown or {},
    }
    if importing is not None:
        change["importing"] = importing
    return {
        "address": address,
        "mode": mode,
        "type": kind,
        "name": address.rsplit(".", 1)[-1],
        "provider_name": "registry.opentofu.org/hashicorp/aws",
        "change": change,
    }


def _rule(days: int) -> list[dict[str, Any]]:
    return [{"id": "VALUE-RULE-ID", "noncurrent_version_expiration": [{"noncurrent_days": days}]}]


LIFECYCLE = "aws_s3_bucket_lifecycle_configuration"
LIFECYCLE_BEFORE = {"bucket": BACKUP_BUCKET, "id": BACKUP_BUCKET, "rule": _rule(60)}
LIFECYCLE_AFTER = {"bucket": BACKUP_BUCKET, "id": BACKUP_BUCKET, "rule": _rule(30)}


def _lifecycle(actions: list[str], **values: Any) -> dict[str, Any]:
    return _entry(
        f"{LIFECYCLE}.backup",
        LIFECYCLE,
        actions,
        values.get("before", LIFECYCLE_BEFORE),
        values.get("after", LIFECYCLE_AFTER),
        values.get("unknown"),
    )


# Every real plan lists each unchanged resource as a no-op, and none of these is a row.
NO_OPS = [
    _entry(
        "aws_iam_role.instance",
        "aws_iam_role",
        ["no-op"],
        {"name": "marketlake-instance", "assume_role_policy": "VALUE-TRUST"},
        {"name": "marketlake-instance", "assume_role_policy": "VALUE-TRUST"},
    ),
    _entry(
        "aws_iam_user_policy.backup",
        "aws_iam_user_policy",
        ["no-op"],
        {"policy": "VALUE-POLICY"},
        {"policy": "VALUE-POLICY"},
    ),
    _entry(
        "aws_s3_bucket.backup",
        "aws_s3_bucket",
        ["no-op"],
        {"bucket": BACKUP_BUCKET, "policy": "VALUE-POLICY"},
        {"bucket": BACKUP_BUCKET, "policy": "VALUE-POLICY"},
    ),
    _entry(
        "aws_s3_bucket_versioning.backup",
        "aws_s3_bucket_versioning",
        ["no-op"],
        {"bucket": BACKUP_BUCKET, "versioning_configuration": [{"status": "Enabled"}]},
        {"bucket": BACKUP_BUCKET, "versioning_configuration": [{"status": "Enabled"}]},
    ),
]


def _plan(*entries: dict[str, Any]) -> dict[str, Any]:
    return {"format_version": "1.2", "resource_changes": [*NO_OPS, *entries]}


# -- running the modes --------------------------------------------------------------


MATCH = object()


class Decision:
    def __init__(self, code: int, verdict: str | None, text: str) -> None:
        self.code = code
        self.verdict = verdict
        self.text = text
        found = re.findall(r"^- Rule (\d), [^:]*: (pass|refuse)", text, re.MULTILINE)
        self.named = [int(number) for number, _ in found]
        self.refused = {int(number) for number, result in found if result == "refuse"}


def _decide(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    plan: dict[str, Any] | str,
    reviewed: Any = MATCH,
    *,
    event: str = "push",
    attempt: str = "1",
    state_bucket: str | None = STATE_BUCKET,
) -> Decision:
    """Run decide mode as the workflow does, with the runner's variables as strings."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    reviewed_path = tmp_path / "reviewed-change-set.json"
    if reviewed is MATCH:
        assert isinstance(plan, dict)
        reviewed = classify.change_set(plan)
    if isinstance(reviewed, bytes):
        reviewed_path.write_bytes(reviewed)
    elif reviewed is not None:
        reviewed_path.write_bytes(classify.encode(reviewed))
    output = tmp_path / "github-output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setenv("GITHUB_EVENT_NAME", event)
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", attempt)
    if state_bucket is None:
        monkeypatch.delenv("TF_STATE_BUCKET", raising=False)
    else:
        monkeypatch.setenv("TF_STATE_BUCKET", state_bucket)
    stdin = io.StringIO(plan if isinstance(plan, str) else json.dumps(plan))
    out = io.StringIO()
    argv = ["classify.py", "decide", "--reviewed", str(reviewed_path)]
    code = classify.main(argv, stdin, out)
    text = out.getvalue()
    assert MARKER not in text
    verdict = None
    if output.exists():
        (line,) = output.read_text().splitlines()
        verdict = line.removeprefix("verdict=")
    return Decision(code, verdict, text)


def _refused(decision: Decision) -> set[int]:
    assert decision.code == 0
    assert decision.named == [1, 2, 3, 4]
    assert decision.verdict == ("apply" if not decision.refused else "refuse")
    return decision.refused


class FakeApi:
    """A stand-in for ``gh api`` that serves bytes by path and records each call."""

    def __init__(self, routes: dict[str, bytes | Exception]) -> None:
        self.routes = routes
        self.calls: list[str] = []

    def __call__(self, path: str) -> bytes:
        self.calls.append(path)
        if path not in self.routes:
            raise _gh_error(b"gh: Not Found (HTTP 404)")
        answer = self.routes[path]
        if isinstance(answer, Exception):
            raise answer
        return answer


def _gh_error(stderr: bytes) -> subprocess.CalledProcessError:
    return subprocess.CalledProcessError(1, ["gh", "api"], output=b"", stderr=stderr)


def _json(data: Any) -> bytes:
    return json.dumps(data).encode()


def _zip(content: bytes, member: str = MEMBER) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, content)
    return buffer.getvalue()


REVIEWED_ROWS = [
    {
        "address": f"{LIFECYCLE}.backup",
        "actions": ["update"],
        "import": False,
        "attributes": ["rule"],
    }
]


def _routes(
    *,
    pulls: Any = None,
    artifacts: list[dict[str, Any]] | None = None,
    runs: dict[int, dict[str, Any]] | None = None,
    blobs: dict[int, bytes | Exception] | None = None,
) -> dict[str, bytes | Exception]:
    artifacts = [ARTIFACT] if artifacts is None else artifacts
    routes: dict[str, bytes | Exception] = {
        PULLS_PATH: _json(PULLS if pulls is None else pulls),
        LISTING_PATH: _json({"total_count": len(artifacts), "artifacts": artifacts}),
    }
    for run_id, run in (runs if runs is not None else {RUN["id"]: RUN}).items():
        routes[_run_path(run_id)] = _json(run)
    blobs = {ARTIFACT["id"]: _zip(classify.encode(REVIEWED_ROWS))} if blobs is None else blobs
    for artifact_id, blob in blobs.items():
        routes[_zip_path(artifact_id)] = blob
    return routes


def _fetch(tmp_path: Path, api: Callable[[str], bytes], repository_id: Any = REPOSITORY_ID):
    out = tmp_path / "reviewed-change-set.json"
    reason = classify.fetch(
        api,
        repository=REPOSITORY,
        sha=MERGE_SHA,
        repository_id=repository_id,
        prefix=PREFIX,
        member=MEMBER,
        out=out,
    )
    assert MARKER not in reason
    return reason, out


def _artifact(**changes: Any) -> dict[str, Any]:
    artifact = copy.deepcopy(ARTIFACT)
    run = changes.pop("workflow_run", {})
    artifact.update(changes)
    artifact["workflow_run"].update(run)
    return artifact


def _run(**changes: Any) -> dict[str, Any]:
    run = copy.deepcopy(RUN)
    run.update(changes)
    return run


# -- the change set -----------------------------------------------------------------


KEYED_PLAN = _plan(
    _entry(
        'aws_s3_bucket_lifecycle_configuration.backup["VALUE-KEY-A"]',
        LIFECYCLE,
        ["update"],
        LIFECYCLE_BEFORE,
        LIFECYCLE_AFTER,
    ),
    _entry(
        "aws_s3_bucket_public_access_block.backup",
        "aws_s3_bucket_public_access_block",
        ["no-op"],
        {"bucket": BACKUP_BUCKET},
        {"bucket": BACKUP_BUCKET},
        importing={"id": BACKUP_BUCKET},
    ),
    _entry(
        "aws_s3_bucket_policy.new",
        "aws_s3_bucket_policy",
        ["create"],
        None,
        {"bucket": BACKUP_BUCKET, "policy": "VALUE-POLICY"},
        {"id": True},
    ),
    _entry(
        'aws_s3_bucket_lifecycle_configuration.backup["VALUE-KEY-B"]',
        LIFECYCLE,
        ["update"],
        LIFECYCLE_BEFORE,
        LIFECYCLE_AFTER,
    ),
    _entry(
        "aws_iam_role.old",
        "aws_iam_role",
        ["delete"],
        {"name": "VALUE-ROLE"},
        None,
    ),
)


def test_the_change_set_lists_rows_sorted_with_keys_redacted_and_duplicates_kept() -> None:
    assert classify.change_set(KEYED_PLAN) == [
        {"address": "aws_iam_role.old", "actions": ["delete"], "import": False, "attributes": []},
        {
            "address": "aws_s3_bucket_lifecycle_configuration.backup[…]",
            "actions": ["update"],
            "import": False,
            "attributes": ["rule"],
        },
        {
            "address": "aws_s3_bucket_lifecycle_configuration.backup[…]",
            "actions": ["update"],
            "import": False,
            "attributes": ["rule"],
        },
        {
            "address": "aws_s3_bucket_policy.new",
            "actions": ["create"],
            "import": False,
            "attributes": [],
        },
        {
            "address": "aws_s3_bucket_public_access_block.backup",
            "actions": ["no-op"],
            "import": True,
            "attributes": [],
        },
    ]


def test_the_change_set_has_one_row_for_each_row_the_summary_prints() -> None:
    plan_summary = sys.modules["plan_summary"]
    for plan in [KEYED_PLAN, _plan(), *[_plan(entry) for _, entry, _ in REFUSED]]:
        assert len(classify.change_set(plan)) == len(plan_summary.rows(plan))


def test_the_change_set_reads_back_from_its_bytes_equal_to_itself() -> None:
    rows = classify.change_set(KEYED_PLAN)
    assert classify.load_change_set(classify.encode(rows)) == rows


def test_one_function_writes_the_bytes() -> None:
    assert classify.encode(REVIEWED_ROWS) == (
        b'[{"actions":["update"],"address":"aws_s3_bucket_lifecycle_configuration.backup",'
        b'"attributes":["rule"],"import":false}]\n'
    )
    redacted = [{"address": "a[…]", "actions": [], "import": True, "attributes": []}]
    assert b"\\u2026" in classify.encode(redacted)


def test_change_set_mode_writes_the_set_and_prints_the_hash_of_its_bytes(tmp_path: Path) -> None:
    out = tmp_path / MEMBER
    summary = io.StringIO()
    argv = ["classify.py", "change-set", "--out", str(out)]
    assert classify.main(argv, io.StringIO(json.dumps(KEYED_PLAN)), summary) == 0
    data = out.read_bytes()
    assert data == classify.encode(classify.change_set(KEYED_PLAN))
    assert f"5 rows, sha256 `{classify.sha256(data)}`." in summary.getvalue()
    assert summary.getvalue().startswith("\n### Change set\n")
    assert MARKER not in summary.getvalue()


@pytest.mark.parametrize(
    "data",
    [
        {},
        [1],
        [{"address": "a", "actions": [], "import": False}],
        [{"address": "a", "actions": [], "import": False, "attributes": [], "extra": 1}],
        [{"address": 1, "actions": [], "import": False, "attributes": []}],
        [{"address": "a", "actions": "update", "import": False, "attributes": []}],
        [{"address": "a", "actions": [], "import": 1, "attributes": []}],
        [{"address": "a", "actions": [], "import": False, "attributes": "rule"}],
        [{"address": "a", "actions": [1], "import": False, "attributes": []}],
        [{"address": "a", "actions": [], "import": False, "attributes": [None]}],
    ],
)
def test_a_corrupt_change_set_is_refused(data: Any) -> None:
    with pytest.raises(ValueError):
        classify.load_change_set(_json(data))


# -- the runner's variables ---------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "number"),
    [("1", 1), ("2", 2), ("1346754080", 1346754080), ("", None), ("x", None), (None, None)],
)
def test_a_runner_variable_becomes_an_integer_or_nothing(value: str | None, number: int) -> None:
    assert classify.runner_int(value) == number


# -- fetch mode ---------------------------------------------------------------------


def test_the_lookup_writes_the_reviewed_set_from_the_pull_requests_own_run(
    tmp_path: Path,
) -> None:
    api = FakeApi(_routes())
    reason, out = _fetch(tmp_path, api)
    data = classify.encode(REVIEWED_ROWS)
    assert out.read_bytes() == data
    assert reason == f"found, from pull request #710, sha256 `{classify.sha256(data)}`."
    assert api.calls == [PULLS_PATH, LISTING_PATH, _run_path(RUN["id"]), _zip_path(ARTIFACT["id"])]


def test_the_repository_id_from_the_runner_is_a_string(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("GITHUB_REPOSITORY", REPOSITORY)
    monkeypatch.setenv("GITHUB_SHA", MERGE_SHA)
    monkeypatch.setenv("GITHUB_REPOSITORY_ID", REPOSITORY_ID)
    out = tmp_path / "reviewed-change-set.json"
    summary = io.StringIO()
    argv = ["classify.py", "fetch", "--name-prefix", PREFIX, "--member", MEMBER]
    argv += ["--out", str(out)]
    assert classify.main(argv, io.StringIO(), summary, api=FakeApi(_routes())) == 0
    assert out.read_bytes() == classify.encode(REVIEWED_ROWS)
    assert summary.getvalue().startswith("\n### Reviewed change set lookup\n\nfound, from")


def _fails(tmp_path: Path, routes: dict[str, bytes | Exception], **kwargs: Any) -> str:
    reason, out = _fetch(tmp_path, FakeApi(routes), **kwargs)
    assert reason.startswith("not found, because ")
    assert not out.exists()
    return reason


NO_RUN = "no unexpired artifact came from this repository's own pull request run"


@pytest.mark.parametrize(
    ("routes", "why"),
    [
        (_routes(pulls=[]), "no pull request merged as this commit"),
        (
            _routes(pulls=[{**PULLS[0], "merge_commit_sha": HEAD_SHA}]),
            "no pull request merged as this commit",
        ),
        (_routes(pulls=PULLS + PULLS), "more than one pull request merged as this commit"),
        (_routes(artifacts=[]), "no artifact is named for the pull request's head"),
        (_routes(runs={RUN["id"]: _run(head_repository={"id": 999})}), NO_RUN),
        (_routes(runs={RUN["id"]: _run(head_repository=None)}), NO_RUN),
        (_routes(runs={RUN["id"]: _run(head_repository={"full_name": REPOSITORY})}), NO_RUN),
        (_routes(runs={RUN["id"]: _run(path=".github/workflows/ci.yml")}), NO_RUN),
        (_routes(runs={RUN["id"]: _run(event="push")}), NO_RUN),
        (_routes(runs={RUN["id"]: _run(head_sha=MERGE_SHA)}), NO_RUN),
        (_routes(artifacts=[_artifact(expired=True)], runs={}), NO_RUN),
        (_routes(artifacts=[{k: v for k, v in ARTIFACT.items() if k != "expired"}]), NO_RUN),
    ],
)
def test_the_lookup_refuses_an_artifact_it_cannot_trace_to_the_reviewed_run(
    tmp_path: Path, routes: dict[str, bytes | Exception], why: str
) -> None:
    assert why in _fails(tmp_path, routes)


def test_an_empty_listing_is_a_missing_artifact(tmp_path: Path) -> None:
    routes = _routes()
    routes[LISTING_PATH] = _json(NO_ARTIFACT)
    assert "no artifact is named" in _fails(tmp_path, routes)


def test_a_forks_newer_artifact_cannot_crowd_out_the_reviewed_one(tmp_path: Path) -> None:
    fork = _artifact(id=2, created_at="2026-10-07T00:00:00Z", workflow_run={"id": 99})
    forged = [{**REVIEWED_ROWS[0], "attributes": ["VALUE-FORGED"]}]
    routes = _routes(
        artifacts=[ARTIFACT, fork],
        runs={RUN["id"]: RUN, 99: _run(id=99, head_repository={"id": 999})},
        blobs={ARTIFACT["id"]: _zip(classify.encode(REVIEWED_ROWS)), 2: _zip(_json(forged))},
    )
    reason, out = _fetch(tmp_path, FakeApi(routes))
    assert reason.startswith("found")
    assert classify.load_change_set(out.read_bytes()) == REVIEWED_ROWS


def test_the_newest_reviewed_artifact_wins(tmp_path: Path) -> None:
    older = _artifact(id=1, created_at="2026-10-06T01:00:00Z")
    newer = _artifact(id=2, created_at="2026-10-06T02:00:00Z")
    newest_rows = [{**REVIEWED_ROWS[0], "attributes": ["expected_bucket_owner", "rule"]}]
    for order in ([older, newer], [newer, older]):
        api = FakeApi(
            _routes(
                artifacts=order,
                blobs={1: _zip(classify.encode(REVIEWED_ROWS)), 2: _zip(_json(newest_rows))},
            )
        )
        _, out = _fetch(tmp_path, api)
        assert classify.load_change_set(out.read_bytes()) == newest_rows
        # Both artifacts came from one run, so the run is read once.
        assert api.calls.count(_run_path(RUN["id"])) == 1
        out.unlink()


def test_an_expired_artifacts_run_is_never_read(tmp_path: Path) -> None:
    api = FakeApi(_routes(artifacts=[_artifact(expired=True)], runs={}))
    _fetch(tmp_path, api)
    assert api.calls == [PULLS_PATH, LISTING_PATH]


@pytest.mark.parametrize(
    ("blob", "error"),
    [
        (_gh_error(b"gh: artifact has expired (HTTP 410)"), "CalledProcessError (HTTP 410)."),
        (_gh_error(b"gh: no status here"), "CalledProcessError."),
        (b"not a zip", "BadZipFile."),
        (_zip(classify.encode(REVIEWED_ROWS), "other.json"), "KeyError."),
        (_zip(b"{not json"), "JSONDecodeError."),
        (_zip(_json([{"address": "a"}])), "ValueError."),
    ],
)
def test_a_failed_download_or_a_bad_artifact_writes_no_file(
    tmp_path: Path, blob: bytes | Exception, error: str
) -> None:
    reason = _fails(tmp_path, _routes(blobs={ARTIFACT["id"]: blob}))
    assert reason.startswith("not found, because the lookup failed with ")
    assert reason.endswith(error)


def test_an_api_error_before_the_download_writes_no_file(tmp_path: Path) -> None:
    routes = _routes()
    routes[PULLS_PATH] = _gh_error(b"gh: API rate limit exceeded (HTTP 403)")
    assert _fails(tmp_path, routes).endswith(
        "the lookup failed with CalledProcessError (HTTP 403)."
    )


@pytest.mark.parametrize("repository_id", ["", "l3a0/marketlake", None])
def test_a_repository_id_that_does_not_parse_writes_no_file(
    tmp_path: Path, repository_id: str | None
) -> None:
    api = FakeApi(_routes())
    reason, out = _fetch(tmp_path, api, repository_id=repository_id)
    assert reason == "not found, because GITHUB_REPOSITORY_ID is not a number."
    assert not out.exists()
    assert api.calls == []


def test_fetch_mode_calls_gh_api_and_survives_its_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    routes = {path: blob for path, blob in _routes().items() if isinstance(blob, bytes)}
    served = tmp_path / "served"
    served.mkdir()
    table = {}
    for index, (path, blob) in enumerate(routes.items()):
        (served / str(index)).write_bytes(blob)
        table[path] = str(served / str(index))
    (served / "table.json").write_text(json.dumps(table))
    gh = tmp_path / "bin" / "gh"
    gh.parent.mkdir()
    gh.write_text(
        f"#!{sys.executable}\n"
        "import json, sys\n"
        f"table = json.load(open({str(served / 'table.json')!r}))\n"
        "assert sys.argv[1] == 'api', sys.argv\n"
        "if sys.argv[2] not in table:\n"
        "    sys.stderr.write('gh: Not Found (HTTP 404)\\n')\n"
        "    sys.exit(1)\n"
        "sys.stdout.buffer.write(open(table[sys.argv[2]], 'rb').read())\n"
    )
    gh.chmod(gh.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{gh.parent}:{os.environ['PATH']}")
    monkeypatch.setenv("GITHUB_REPOSITORY", REPOSITORY)
    monkeypatch.setenv("GITHUB_REPOSITORY_ID", REPOSITORY_ID)
    argv = ["classify.py", "fetch", "--name-prefix", PREFIX, "--member", MEMBER, "--out"]

    monkeypatch.setenv("GITHUB_SHA", MERGE_SHA)
    out = tmp_path / "found.json"
    summary = io.StringIO()
    assert classify.main([*argv, str(out)], io.StringIO(), summary) == 0
    assert out.read_bytes() == classify.encode(REVIEWED_ROWS)

    monkeypatch.setenv("GITHUB_SHA", HEAD_SHA)
    out = tmp_path / "missing.json"
    summary = io.StringIO()
    assert classify.main([*argv, str(out)], io.StringIO(), summary) == 0
    assert not out.exists()
    assert "the lookup failed with CalledProcessError (HTTP 404)." in summary.getvalue()


# -- decide mode: what refuses ------------------------------------------------------


REFUSED: list[tuple[str, dict[str, Any], set[int]]] = [
    ("a delete", _lifecycle(["delete"], after=None), {2}),
    ("a replace, delete first", _lifecycle(["delete", "create"]), {2}),
    ("a replace, create first", _lifecycle(["create", "delete"]), {2}),
    ("a forget", _lifecycle(["forget"], after=None), {2}),
    ("a forget then create", _lifecycle(["forget", "create"]), {2}),
    ("an unknown action", _lifecycle(["archive"]), {2}),
    (
        "an aws_iam_ update",
        _entry(
            "aws_iam_role.instance",
            "aws_iam_role",
            ["update"],
            {"name": "marketlake-instance", "max_session_duration": 3600},
            {"name": "marketlake-instance", "max_session_duration": 7200},
        ),
        {3, 4},
    ),
    (
        "an aws_kms_ change",
        _entry(
            "aws_kms_key.lake",
            "aws_kms_key",
            ["update"],
            {"description": "VALUE-OLD"},
            {"description": "VALUE-NEW"},
        ),
        {3, 4},
    ),
    (
        "a state-bucket update",
        _lifecycle(
            ["update"],
            before={**LIFECYCLE_BEFORE, "bucket": STATE_BUCKET},
            after={**LIFECYCLE_AFTER, "bucket": STATE_BUCKET},
        ),
        {3},
    ),
    (
        "a state-bucket create",
        _lifecycle(["create"], before=None, after={**LIFECYCLE_AFTER, "bucket": STATE_BUCKET}),
        {3},
    ),
    (
        "a state-bucket delete",
        _lifecycle(["delete"], before={**LIFECYCLE_BEFORE, "bucket": STATE_BUCKET}, after=None),
        {2, 3},
    ),
    (
        "an unknown bucket",
        _lifecycle(
            ["create"],
            before=None,
            after={"rule": _rule(30)},
            unknown={"bucket": True, "id": True},
        ),
        {3},
    ),
    (
        "a changed policy attribute",
        _entry(
            "aws_s3_bucket_policy.backup",
            "aws_s3_bucket_policy",
            ["update"],
            {"bucket": BACKUP_BUCKET, "policy": "VALUE-OLD"},
            {"bucket": BACKUP_BUCKET, "policy": "VALUE-NEW"},
        ),
        {3, 4},
    ),
    (
        "a changed assume_role_policy",
        _entry(
            "aws_iam_role.instance",
            "aws_iam_role",
            ["update"],
            {"name": "marketlake-instance", "assume_role_policy": "VALUE-OLD"},
            {"name": "marketlake-instance", "assume_role_policy": "VALUE-NEW"},
        ),
        {3, 4},
    ),
    (
        "a created aws_s3_bucket_policy",
        _entry(
            "aws_s3_bucket_policy.backup",
            "aws_s3_bucket_policy",
            ["create"],
            None,
            {"bucket": BACKUP_BUCKET, "policy": "VALUE-POLICY"},
            {"id": True},
        ),
        {3, 4},
    ),
    (
        "a created aws_s3_bucket with a computed policy",
        _entry(
            "aws_s3_bucket.new",
            "aws_s3_bucket",
            ["create"],
            None,
            {"bucket": "VALUE-NEW-BUCKET", "force_destroy": False, "tags": None},
            {"arn": True, "id": True, "policy": True, "region": True},
        ),
        {3, 4},
    ),
    (
        "a data.aws_iam_policy_document read",
        _entry(
            "data.aws_iam_policy_document.instance",
            "aws_iam_policy_document",
            ["read"],
            None,
            {"version": "2012-10-17", "statement": [{"actions": ["s3:GetObject"]}]},
            {"id": True, "json": True, "minified_json": True},
            mode="data",
        ),
        {3, 4},
    ),
    (
        "an aws_s3_bucket_versioning update",
        _entry(
            "aws_s3_bucket_versioning.backup",
            "aws_s3_bucket_versioning",
            ["update"],
            {"bucket": BACKUP_BUCKET, "versioning_configuration": [{"status": "Enabled"}]},
            {"bucket": BACKUP_BUCKET, "versioning_configuration": [{"status": "Suspended"}]},
        ),
        {4},
    ),
    (
        "an aws_instance update",
        _entry(
            "aws_instance.capture",
            "aws_instance",
            ["update"],
            {"instance_type": "t4g.small", "user_data": "VALUE-OLD"},
            {"instance_type": "t4g.medium", "user_data": "VALUE-OLD"},
        ),
        {4},
    ),
    (
        "an aws_ec2_instance_state create",
        _entry(
            "aws_ec2_instance_state.capture",
            "aws_ec2_instance_state",
            ["create"],
            None,
            {"instance_id": "VALUE-INSTANCE", "state": "stopped", "force": False},
            {"id": True},
        ),
        {4},
    ),
    (
        "an aws_security_group update",
        _entry(
            "aws_security_group.capture",
            "aws_security_group",
            ["update"],
            {"name": "capture", "egress": [{"cidr_blocks": ["VALUE-CIDR"]}]},
            {"name": "capture", "egress": []},
        ),
        {4},
    ),
]


@pytest.mark.parametrize(
    ("entry", "refused"),
    [(entry, refused) for _, entry, refused in REFUSED],
    ids=[name for name, _, _ in REFUSED],
)
def test_each_refused_case_names_the_rules_that_refuse_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, entry: dict[str, Any], refused: set[int]
) -> None:
    decision = _decide(tmp_path, monkeypatch, _plan(entry))
    assert _refused(decision) == refused
    assert "Reviewed change set: found, matched." in decision.text
    address = classify.address_without_keys(entry["address"])
    for number in refused:
        assert re.search(
            rf"^- Rule {number}, [^:]*: refuse, refused by `{re.escape(address)}`\.$",
            decision.text,
            re.MULTILINE,
        )
    assert decision.text.rstrip().endswith(
        "Verdict: refuse. The `apply` job waits for the owner's approval in `infra`."
    )


def _case(name: str) -> dict[str, Any]:
    (entry,) = [entry for case, entry, _ in REFUSED if case == name]
    return entry


# Rule 3 is a floor that a later allowlist entry cannot lift, so its checks get fixtures
# that rule 4 does not also refuse.
FLOOR = [
    (
        "block_public_policy on the allowlisted public access block",
        set(),
        _entry(
            "aws_s3_bucket_public_access_block.backup",
            "aws_s3_bucket_public_access_block",
            ["update"],
            {"bucket": BACKUP_BUCKET, "block_public_policy": True},
            {"bucket": BACKUP_BUCKET, "block_public_policy": False},
        ),
    ),
    ("an aws_iam_ update", {"aws_iam_role"}, _case("an aws_iam_ update")),
    (
        "a created aws_s3_bucket_policy",
        {"aws_s3_bucket_policy"},
        _case("a created aws_s3_bucket_policy"),
    ),
    (
        "a created aws_s3_bucket with a computed policy",
        {"aws_s3_bucket"},
        _case("a created aws_s3_bucket with a computed policy"),
    ),
]


@pytest.mark.parametrize(
    ("admitted", "entry"),
    [(admitted, entry) for _, admitted, entry in FLOOR],
    ids=[name for name, _, _ in FLOOR],
)
def test_rule_3_refuses_what_the_allowlist_admits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    admitted: set[str],
    entry: dict[str, Any],
) -> None:
    monkeypatch.setattr(classify, "ALLOWLIST", classify.ALLOWLIST | admitted)
    assert _refused(_decide(tmp_path, monkeypatch, _plan(entry))) == {3}


UPDATE = _plan(_lifecycle(["update"]))


@pytest.mark.parametrize(
    ("reviewed", "event", "attempt", "found", "why"),
    [
        (
            [{**REVIEWED_ROWS[0], "attributes": ["expected_bucket_owner", "rule"]}],
            "push",
            "1",
            "found, differs",
            "the change set differs from the reviewed one",
        ),
        (
            REVIEWED_ROWS + REVIEWED_ROWS,
            "push",
            "1",
            "found, differs",
            "the change set differs from the reviewed one",
        ),
        (None, "push", "1", "not found", "no reviewed change set was found"),
        (b"[{]", "push", "1", "unreadable", "no reviewed change set was found"),
        (MATCH, "push", "2", "found, matched", "a re-run is never approved by the merge"),
        (MATCH, "push", "x", "found, matched", "a re-run is never approved by the merge"),
        (
            MATCH,
            "workflow_dispatch",
            "1",
            "found, matched",
            "only a push has a reviewed change set",
        ),
    ],
    ids=[
        "a different change set",
        "a row reviewed twice and planned once",
        "no reviewed file",
        "an unreadable reviewed file",
        "a re-run",
        "an attempt that does not parse",
        "a workflow_dispatch run",
    ],
)
def test_a_plan_that_changes_something_needs_its_reviewed_set_on_a_first_push(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reviewed: Any,
    event: str,
    attempt: str,
    found: str,
    why: str,
) -> None:
    assert classify.change_set(UPDATE) == REVIEWED_ROWS
    decision = _decide(tmp_path, monkeypatch, UPDATE, reviewed, event=event, attempt=attempt)
    assert _refused(decision) == {1}
    assert f"Reviewed change set: {found}." in decision.text
    assert f"- Rule 1, the change set is empty or the reviewed one: refuse, {why}." in (
        decision.text
    )


def test_a_no_op_import_is_a_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plan = _plan(KEYED_PLAN["resource_changes"][5])
    assert classify.change_set(plan) != []
    assert _refused(_decide(tmp_path / "a", monkeypatch, plan, None)) == {1}
    assert _refused(_decide(tmp_path / "b", monkeypatch, plan)) == set()


# -- decide mode: what applies ------------------------------------------------------


@pytest.mark.parametrize(
    ("reviewed", "event", "attempt", "found"),
    [
        ([], "push", "1", "found, matched"),
        (None, "push", "1", "not found"),
        (None, "workflow_dispatch", "1", "not found"),
        (None, "push", "2", "not found"),
        (REVIEWED_ROWS, "push", "1", "found, differs"),
    ],
    ids=[
        "its own empty reviewed set",
        "a Dependabot merge with no artifact",
        "a workflow_dispatch run",
        "a re-run",
        "a stale reviewed set",
    ],
)
def test_a_plan_that_changes_nothing_applies_whatever_was_reviewed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reviewed: Any,
    event: str,
    attempt: str,
    found: str,
) -> None:
    decision = _decide(tmp_path, monkeypatch, _plan(), reviewed, event=event, attempt=attempt)
    assert _refused(decision) == set()
    assert f"Reviewed change set: {found}." in decision.text
    assert (
        "- Rule 1, the change set is empty or the reviewed one: pass, the plan changes nothing."
        in decision.text
    )
    assert "- Rule 2, nothing is destroyed or replaced: pass." in decision.text
    assert decision.text.rstrip().endswith("Verdict: apply with no click.")


def _merged_plan(days: int) -> dict[str, Any]:
    """Two keyed lifecycle updates that redact to one address, and an encryption update."""
    after = {**LIFECYCLE_AFTER, "rule": _rule(days)}
    return _plan(
        _entry(
            f'{LIFECYCLE}.backup["VALUE-KEY-A"]', LIFECYCLE, ["update"], LIFECYCLE_BEFORE, after
        ),
        _entry(
            f'{LIFECYCLE}.backup["VALUE-KEY-B"]', LIFECYCLE, ["update"], LIFECYCLE_BEFORE, after
        ),
        _entry(
            "aws_s3_bucket_server_side_encryption_configuration.backup",
            "aws_s3_bucket_server_side_encryption_configuration",
            ["update"],
            {"bucket": BACKUP_BUCKET, "rule": [{"bucket_key_enabled": False}]},
            {"bucket": BACKUP_BUCKET, "rule": [{"bucket_key_enabled": days > 0}]},
        ),
    )


def test_a_reviewed_lifecycle_update_applies_after_crossing_every_file_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The pull request's plan job writes the change set, and upload-artifact zips it.
    uploaded = tmp_path / MEMBER
    summary = io.StringIO()
    argv = ["classify.py", "change-set", "--out", str(uploaded)]
    assert classify.main(argv, io.StringIO(json.dumps(_merged_plan(30))), summary) == 0
    digest = classify.sha256(uploaded.read_bytes())
    assert f"3 rows, sha256 `{digest}`." in summary.getvalue()

    # main's apply-auto job finds it through the merge commit.
    routes = _routes(blobs={ARTIFACT["id"]: _zip(uploaded.read_bytes())})
    reason, reviewed = _fetch(tmp_path, FakeApi(routes))
    assert reason == f"found, from pull request #710, sha256 `{digest}`."

    # main's plan changes the same attributes to other values, which still matches.
    decision = _decide(tmp_path, monkeypatch, _merged_plan(45), reviewed.read_bytes())
    assert _refused(decision) == set()
    assert "Reviewed change set: found, matched." in decision.text
    assert f"This plan's change set: 3 rows, sha256 `{digest}`." in decision.text
    assert "- Rule 1, the change set is empty or the reviewed one: pass, the change set " in (
        decision.text
    )
    assert decision.text.rstrip().endswith("Verdict: apply with no click.")


# -- decide mode: malformed input ---------------------------------------------------


@pytest.mark.parametrize(
    ("plan", "state_bucket", "error"),
    [
        ("{not json", STATE_BUCKET, "JSONDecodeError"),
        (
            json.dumps(_plan({"address": "VALUE-ADDRESS", "change": {"actions": ["update"]}})),
            STATE_BUCKET,
            "KeyError",
        ),
        (json.dumps({"resource_changes": [{"address": "x"}]}), STATE_BUCKET, "KeyError"),
        (json.dumps(_plan()), None, "KeyError"),
        (json.dumps(_plan()), "", "ValueError"),
    ],
    ids=[
        "not JSON",
        "a row with no type",
        "an entry with no change",
        "no state bucket",
        "an empty state bucket",
    ],
)
def test_malformed_input_fails_the_job_and_names_only_the_exception_type(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    plan: str,
    state_bucket: str | None,
    error: str,
) -> None:
    decision = _decide(tmp_path, monkeypatch, plan, None, state_bucket=state_bucket)
    assert decision.code == 1
    assert decision.verdict is None
    assert decision.text == f"\n### No-click apply\n\nmalformed input: {error}\n"
