"""``infra/ci/plan_summary.py`` turns a plan into rows and leaks no value.

The script first runs inside the infra workflow, whose step summary anyone can read, so
these literal plans stand in for ``tofu show -json``. Every value in them is a marker
string, and each test checks that no marker reaches the output.
"""

from __future__ import annotations

import importlib.util
import io
import json
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "infra" / "ci" / "plan_summary.py"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("plan_summary", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


plan_summary = _load()

# Values that must never reach the summary.
SECRETS = [
    "VALUE-BUCKET-NAME",
    "VALUE-OLD-RULE",
    "VALUE-NEW-RULE",
    "VALUE-OLD-TRUST",
    "VALUE-NEW-TRUST",
    "VALUE-POLICY",
    "VALUE-DELETED",
    "VALUE-IMPORT-ID",
    "VALUE-KEY",
]


def _change(address: str, actions: list[str], **change: Any) -> dict[str, Any]:
    return {"address": address, "change": {"actions": actions, **change}}


PLAN = {
    "format_version": "1.2",
    "resource_changes": [
        _change(
            "aws_iam_role_policy.instance_s3[0]",
            ["create"],
            before=None,
            after={"name": "VALUE-POLICY", "policy": "VALUE-POLICY"},
            after_unknown={"id": True},
        ),
        _change(
            "aws_s3_bucket_lifecycle_configuration.backup",
            ["update"],
            before={"bucket": "VALUE-BUCKET-NAME", "rule": ["VALUE-OLD-RULE"]},
            after={"bucket": "VALUE-BUCKET-NAME", "rule": ["VALUE-NEW-RULE"]},
            after_unknown={},
        ),
        _change(
            "aws_iam_role.instance",
            ["delete", "create"],
            before={"name": "marketlake-instance", "assume_role_policy": "VALUE-OLD-TRUST"},
            after={"name": "marketlake-instance", "assume_role_policy": "VALUE-NEW-TRUST"},
            after_unknown={"arn": True, "unique_id": True},
        ),
        _change(
            "aws_iam_instance_profile.instance",
            ["delete"],
            before={"name": "VALUE-DELETED"},
            after=None,
            after_unknown={},
        ),
        _change(
            "aws_s3_bucket.backup",
            ["no-op"],
            before={"bucket": "VALUE-BUCKET-NAME"},
            after={"bucket": "VALUE-BUCKET-NAME"},
            importing={"id": "VALUE-IMPORT-ID"},
        ),
        _change(
            'aws_iam_role_policy.keyed["VALUE-KEY"]',
            ["update"],
            before={"policy": "VALUE-OLD-RULE"},
            after={"policy": "VALUE-NEW-RULE"},
            after_unknown={},
        ),
        _change(
            "aws_iam_role.unchanged",
            ["no-op"],
            before={"name": "VALUE-BUCKET-NAME"},
            after={"name": "VALUE-BUCKET-NAME"},
        ),
    ],
}


def _run(plan: dict[str, Any], *argv: str) -> str:
    out = io.StringIO()
    assert plan_summary.main(["plan_summary.py", *argv], io.StringIO(json.dumps(plan)), out) == 0
    return out.getvalue()


def test_rows_name_address_action_and_changed_attributes() -> None:
    assert plan_summary.rows(PLAN) == [
        ("aws_iam_role_policy.instance_s3[…]", "create", ""),
        ("aws_s3_bucket_lifecycle_configuration.backup", "update", "rule"),
        ("aws_iam_role.instance", "replace", "arn, assume_role_policy, unique_id"),
        ("aws_iam_instance_profile.instance", "delete", ""),
        ("aws_s3_bucket.backup", "no-op (import)", ""),
        ("aws_iam_role_policy.keyed[…]", "update", "policy"),
    ]


def test_an_import_that_changes_attributes_is_marked_and_named() -> None:
    plan = {
        "resource_changes": [
            _change(
                "aws_iam_user_policy.backup",
                ["update"],
                before={"name": "VALUE-POLICY", "policy": "VALUE-OLD-RULE"},
                after={"name": "VALUE-POLICY", "policy": "VALUE-NEW-RULE"},
                after_unknown={},
                importing={"id": "VALUE-IMPORT-ID"},
            )
        ]
    }
    assert plan_summary.rows(plan) == [("aws_iam_user_policy.backup", "update (import)", "policy")]


def test_the_table_is_markdown() -> None:
    lines = _run(PLAN, "Plan of infra/live").splitlines()
    assert lines[:4] == [
        "### Plan of infra/live",
        "",
        "| Address | Action | Changed attributes |",
        "| --- | --- | --- |",
    ]
    assert lines[4] == "| `aws_iam_role_policy.instance_s3[…]` | create |  |"
    assert lines[8] == "| `aws_s3_bucket.backup` | no-op (import) |  |"
    assert lines[9] == "| `aws_iam_role_policy.keyed[…]` | update | policy |"
    assert len(lines) == 10


@pytest.mark.parametrize("secret", SECRETS)
def test_no_value_reaches_the_output(secret: str) -> None:
    assert secret in json.dumps(PLAN)
    assert secret not in _run(PLAN)


@pytest.mark.parametrize(
    ("address", "expected"),
    [
        ("aws_x.y", "aws_x.y"),
        ("aws_x.y[0]", "aws_x.y[…]"),
        ('aws_x.y["VALUE-KEY"]', "aws_x.y[…]"),
        ('aws_x.y["VALUE-KEY \\"]\\" VALUE-KEY"]', "aws_x.y[…]"),
        ('module.m["VALUE-KEY"].aws_x.y["VALUE-KEY"]', "module.m[…].aws_x.y[…]"),
    ],
)
def test_a_key_never_reaches_the_address(address: str, expected: str) -> None:
    assert plan_summary.address_without_keys(address) == expected
    plan = {"resource_changes": [_change(address, ["create"], before=None, after={})]}
    assert "VALUE-KEY" not in _run(plan)


def test_an_empty_plan_says_so() -> None:
    assert _run({"resource_changes": []}) == "### Plan\n\nNo changes.\n"
    assert _run({"format_version": "1.2"}) == "### Plan\n\nNo changes.\n"
