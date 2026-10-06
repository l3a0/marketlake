"""Checks on ``infra/`` that ``tofu test`` cannot make, read from the ``.tf`` files.

``tofu test`` sees one configuration's plan, and six things are not in one.

1. ``prevent_destroy``. A test refuses destroy-mode plans, and ``tofu show -json`` omits
   ``lifecycle``, so removing the line leaves every ``tofu test`` run green.
2. Every policy attached to a role. An assert names the attachments it knows about, so
   a second attachment of the same type passes it. The plan role is trusted on every
   pull request from a branch here with no approval, so any write it gains lets an
   unreviewed branch change the account.
3. Whether the live backend's key is the one the apply role may write. A mismatch
   passes every pull request check and fails the first apply after the merge.
4. The backend's own settings. Without ``use_lockfile`` two applies can write the state
   at once, and without ``encrypt`` it lands unencrypted.
5. Whether the IAM names the apply role may write in ``infra/bootstrap`` are the names
   ``infra/live`` declares. Each configuration's tests see only their own side.
6. Whether every managed policy ``infra/live`` attaches is one the apply role may
   attach to that role. A plan only reads, so an ARN the apply role's ``iam:PolicyARN``
   condition refuses passes every pull request check and first fails with an
   AccessDenied at the apply after the merge.

A ``module`` block would hide its resources from every check here, so neither
configuration may call one.

These run in ``ci.yml``'s required ``test`` job, which has no OpenTofu. The parse is
``python-hcl2``'s, which keeps a function call such as ``jsonencode({...})`` as text, so
:func:`_jsonencode_argument` parses the call's argument as HCL on its own.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import hcl2
import pytest

INFRA = Path(__file__).resolve().parents[2] / "infra"

_OPTIONS = hcl2.SerializationOptions(
    strip_string_quotes=True, explicit_blocks=False, with_comments=False
)

# Price 4 of issue #664: every resource whose loss would lose backups or stop them.
PREVENT_DESTROY = [
    "bootstrap/aws_s3_bucket.state",
    "bootstrap/aws_s3_bucket_versioning.state",
    "live/aws_s3_bucket.backup",
    "live/aws_s3_bucket_versioning.backup",
    "live/aws_s3_bucket_server_side_encryption_configuration.backup",
    "live/aws_s3_bucket_public_access_block.backup",
    "live/aws_s3_bucket_lifecycle_configuration.backup",
    "live/aws_iam_user.backup",
    "live/aws_iam_user_policy.backup",
]

# Every resource type the bootstrap may hold. A new type, such as a second way to
# attach a policy, fails here until this list and the role checks below account for it.
BOOTSTRAP_TYPES = {
    "aws_s3_bucket",
    "aws_s3_bucket_versioning",
    "aws_iam_openid_connect_provider",
    "aws_iam_role",
    "aws_iam_role_policy",
    "aws_iam_role_policy_attachment",
}

READ_ONLY = "arn:aws:iam::aws:policy/ReadOnlyAccess"


def _resources(config: str) -> dict[str, dict[str, Any]]:
    """Every resource block in one configuration, keyed ``type.name``."""
    found: dict[str, dict[str, Any]] = {}
    for path in sorted((INFRA / config).glob("*.tf")):
        with path.open() as f:
            parsed = hcl2.load(f, serialization_options=_OPTIONS)
        for block in parsed.get("resource", []):
            for rtype, named in block.items():
                for name, body in named.items():
                    address = f"{rtype}.{name}"
                    assert address not in found, f"{config}/{address} is declared twice"
                    found[address] = body
    return found


def _backend(config: str) -> dict[str, Any]:
    for path in sorted((INFRA / config).glob("*.tf")):
        with path.open() as f:
            parsed = hcl2.load(f, serialization_options=_OPTIONS)
        for block in parsed.get("terraform", []):
            for backend in block.get("backend", []):
                return backend["s3"]
    raise AssertionError(f"infra/{config} has no s3 backend")


def _backend_key(config: str) -> str:
    return _backend(config)["key"]


def _jsonencode_argument(expression: str) -> Any:
    """The literal a ``jsonencode(...)`` expression encodes, parsed as HCL."""
    prefix, suffix = "${jsonencode(", ")}"
    assert expression.startswith(prefix) and expression.endswith(suffix), (
        f"a policy must be a jsonencode() literal, got {expression[:40]!r}"
    )
    inner = expression[len(prefix) : -len(suffix)]
    return hcl2.loads(f"value = {inner}", serialization_options=_OPTIONS)["value"]


def _role_name(reference: str) -> str:
    """The resource name in ``${aws_iam_role.<name>.name}`` or ``.id``."""
    match = re.fullmatch(r"\$\{aws_iam_role\.(\w+)\.(?:name|id)\}", reference)
    assert match, f"a role must be referenced through its resource, got {reference!r}"
    return match.group(1)


def _actions(statement: dict[str, Any]) -> list[str]:
    action = statement["Action"]
    return [action] if isinstance(action, str) else list(action)


@pytest.mark.parametrize("address", PREVENT_DESTROY)
def test_resource_carries_prevent_destroy(address: str) -> None:
    config, resource = address.split("/")
    body = _resources(config).get(resource)
    assert body is not None, f"infra/{address} is missing"
    lifecycles = body.get("lifecycle", [])
    assert any(block.get("prevent_destroy") is True for block in lifecycles), (
        f"infra/{address} does not carry prevent_destroy = true"
    )


@pytest.mark.parametrize("config", ["bootstrap", "live"])
def test_configuration_calls_no_module(config: str) -> None:
    """A module's resources are invisible to :func:`_resources`, so a policy attached
    inside one would pass every check here."""
    for path in sorted((INFRA / config).glob("*.tf")):
        with path.open() as f:
            parsed = hcl2.load(f, serialization_options=_OPTIONS)
        assert not parsed.get("module"), f"infra/{config}/{path.name} calls a module"


def test_bootstrap_holds_only_known_resource_types() -> None:
    types = {address.split(".")[0] for address in _resources("bootstrap")}
    assert types == BOOTSTRAP_TYPES


def test_bootstrap_roles_carry_exactly_their_policies() -> None:
    resources = _resources("bootstrap")
    roles = sorted(a.split(".")[1] for a in resources if a.startswith("aws_iam_role."))
    assert roles == ["apply", "plan"]
    for role in roles:
        body = resources[f"aws_iam_role.{role}"]
        assert "managed_policy_arns" not in body and "inline_policy" not in body

    attached: list[tuple[str, str]] = []
    inline: dict[str, list[list[dict[str, Any]]]] = {}
    for address, body in resources.items():
        rtype = address.split(".")[0]
        if rtype not in ("aws_iam_role_policy_attachment", "aws_iam_role_policy"):
            continue
        # One block must mean one attachment.
        assert "count" not in body and "for_each" not in body, address
        role = _role_name(body["role"])
        if rtype == "aws_iam_role_policy_attachment":
            attached.append((role, body["policy_arn"]))
        else:
            inline.setdefault(role, []).append(_jsonencode_argument(body["policy"])["Statement"])

    assert sorted(attached) == [("apply", READ_ONLY), ("plan", READ_ONLY)]
    assert sorted(inline) == ["apply", "plan"]
    assert len(inline["apply"]) == 1
    assert len(inline["plan"]) == 1
    plan_effects = [statement["Effect"] for statement in inline["plan"][0]]
    assert plan_effects and set(plan_effects) == {"Deny"}


def test_apply_role_writes_exactly_the_live_state_and_its_lock() -> None:
    live_key = _backend_key("live")
    assert live_key == "live/terraform.tfstate"
    assert _backend_key("bootstrap") == "bootstrap/terraform.tfstate"

    policy = _jsonencode_argument(_resources("bootstrap")["aws_iam_role_policy.apply"]["policy"])
    object_writes = [
        statement
        for statement in policy["Statement"]
        if statement["Effect"] == "Allow"
        and {"s3:PutObject", "s3:DeleteObject"} & set(_actions(statement))
    ]
    assert len(object_writes) == 1
    assert sorted(_actions(object_writes[0])) == ["s3:DeleteObject", "s3:PutObject"]
    assert object_writes[0]["Resource"] == [
        f"arn:aws:s3:::${{var.state_bucket}}/{live_key}",
        f"arn:aws:s3:::${{var.state_bucket}}/{live_key}.tflock",
    ]


@pytest.mark.parametrize("config", ["bootstrap", "live"])
def test_backend_locks_and_encrypts_the_state(config: str) -> None:
    backend = _backend(config)
    assert backend["use_lockfile"] is True
    assert backend["encrypt"] is True


# The ARN kind in an apply-role grant, and the resource type that declares it in live.
_IAM_KINDS = {
    "role": "aws_iam_role",
    "instance-profile": "aws_iam_instance_profile",
    "user": "aws_iam_user",
}


def test_apply_role_grants_name_the_iam_resources_live_declares() -> None:
    """A rename on either side passes both configurations' own tests and fails the
    first apply after the merge, with an AccessDenied on the renamed resource."""
    policy = _jsonencode_argument(_resources("bootstrap")["aws_iam_role_policy.apply"]["policy"])
    granted = set()
    for statement in policy["Statement"]:
        if statement["Effect"] != "Allow":
            continue
        for resource in statement["Resource"]:
            match = re.fullmatch(r"arn:aws:iam::\$\{local\.account_id\}:([a-z-]+)/(.+)", resource)
            if match:
                granted.add((match.group(1), match.group(2)))

    declared = {
        (kind, body["name"])
        for kind, rtype in _IAM_KINDS.items()
        for address, body in _resources("live").items()
        if address.split(".")[0] == rtype
    }
    assert granted == {
        ("role", "marketlake-instance"),
        ("instance-profile", "marketlake-instance"),
        ("user", "marketlake-backup"),
    }
    assert declared == granted


# The condition operators under which an ``iam:PolicyARN`` value grants that ARN. A
# negated operator such as ``ArnNotEquals`` grants every other ARN instead.
_GRANTING_OPERATORS = {"ArnEquals", "StringEquals"}


def _listed(value: str | list[str]) -> list[str]:
    return [value] if isinstance(value, str) else list(value)


def test_apply_role_may_attach_every_policy_live_attaches() -> None:
    """Each attachment needs one Allow statement that grants ``iam:AttachRolePolicy`` on
    its role's name and names its ARN under a granting operator. Collecting every
    ``iam:PolicyARN`` in the policy instead would pass a Deny statement, a negated
    operator and an Allow that grants only ``iam:DetachRolePolicy``."""
    live = _resources("live")
    attachments = {
        address: body
        for address, body in live.items()
        if address.split(".")[0] == "aws_iam_role_policy_attachment"
    }
    assert attachments, "infra/live attaches no managed policy, so this check reads nothing"

    policy = _jsonencode_argument(_resources("bootstrap")["aws_iam_role_policy.apply"]["policy"])
    grants: list[tuple[set[str], set[str]]] = []
    for statement in policy["Statement"]:
        if statement["Effect"] != "Allow" or "iam:AttachRolePolicy" not in _actions(statement):
            continue
        roles = set()
        for resource in _listed(statement["Resource"]):
            match = re.fullmatch(r"arn:aws:iam::\$\{local\.account_id\}:role/(.+)", resource)
            if match:
                roles.add(match.group(1))
        arns = set()
        for operator, keys in statement.get("Condition", {}).items():
            if operator not in _GRANTING_OPERATORS:
                continue
            for key, value in keys.items():
                if key.lower() == "iam:policyarn":
                    arns.update(_listed(value))
        grants.append((roles, arns))

    for address, body in attachments.items():
        role = live[f"aws_iam_role.{_role_name(body['role'])}"]["name"]
        arn = body["policy_arn"]
        assert any(role in roles and arn in arns for roles, arns in grants), (
            f"infra/live/{address} attaches {arn} to {role}, which the apply role may not"
        )
