"""Checks on ``infra/`` that ``tofu test`` cannot make, read from the ``.tf`` files.

``tofu test`` sees one configuration's plan, and fourteen things are not in one.

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
7. Whether anything in ``infra/live`` manages a role's policies exclusively.
   ``managed_policy_arns`` on a role, or a resource type such as
   ``aws_iam_role_policy_attachments_exclusive``, detaches every managed policy it does
   not list on each apply, the SSM policy included. An assert can only name what is
   present, so an added argument or resource type passes every ``tofu test`` run.
8. Every policy on an IAM user in ``infra/live``. An assert names the policy it checks,
   so a second inline policy on the same user passes it, and the apply role's
   ``iam:PutUserPolicy`` would apply it.
9. Whether either configuration declares an SSM parameter as a resource or a data
   source. A data source, or a resource that takes its value the ordinary way, puts the
   decrypted value into the state, which every pull request's plan role can read, and a
   mock provider plans it without complaint. An ephemeral block stores nothing, so it
   stays allowed.
10. The VM's ``ignore_changes`` list, which ``lifecycle`` keeps out of the plan as it
    does ``prevent_destroy``. Without an entry, a new AMI or a stop that drops the
    public address plans a replacement of the instance (#686).
11. Whether two inline policies on one role share a name. ``PutRolePolicy`` on a shared
    name makes one overwrite the other, and a mock provider plans both.
12. Whether the lake volume's zone is one literal that the subnet lookup shares. A
    variable would offer a change ``prevent_destroy`` refuses, and the mock plans any
    zone.
13. Arguments that must stay absent. ``disable_api_stop`` makes the attachment's stop
    before a detach fail, and a ``kms_key_id`` reads back as an ARN, so every plan
    shows a change.
14. The shim template's variables. A third one could carry a value into ``user_data``,
    which anyone who can describe the instance reads, and the plan shows only the
    rendered text.

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

# Price 4 of issue #664: every resource whose loss would lose backups or stop them. The
# token writer and its policy are here too (#699). CI cannot delete either, so a pull
# request that renames one fails at plan rather than at apply.
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
    "live/aws_iam_user.token_writer",
    "live/aws_iam_user_policy.token_writer",
    # The lake volume holds every minute captured since the last nightly upload (#686).
    "live/aws_ebs_volume.lake",
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

# Every resource type infra/live may hold. A new type, such as one that manages a role's
# attachments exclusively, fails here until this list and the checks below account for it.
LIVE_TYPES = {
    "aws_s3_bucket",
    "aws_s3_bucket_versioning",
    "aws_s3_bucket_server_side_encryption_configuration",
    "aws_s3_bucket_public_access_block",
    "aws_s3_bucket_lifecycle_configuration",
    "aws_iam_user",
    "aws_iam_user_policy",
    "aws_iam_role",
    "aws_iam_role_policy",
    "aws_iam_role_policy_attachment",
    "aws_iam_instance_profile",
    "aws_instance",
    "aws_security_group",
    "aws_key_pair",
    "aws_ebs_volume",
    "aws_volume_attachment",
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


# Each of these puts a parameter's decrypted value into the state (#699). An ephemeral
# block reads a value without storing it, so it stays allowed.
_PARAMETER_VALUE_TYPES = {"aws_ssm_parameter", "aws_ssm_parameters_by_path"}


@pytest.mark.parametrize("config", ["bootstrap", "live"])
def test_configuration_keeps_parameter_values_out_of_state(config: str) -> None:
    for path in sorted((INFRA / config).glob("*.tf")):
        with path.open() as f:
            parsed = hcl2.load(f, serialization_options=_OPTIONS)
        for kind in ("resource", "data"):
            declared = {rtype for block in parsed.get(kind, []) for rtype in block}
            assert not declared & _PARAMETER_VALUE_TYPES, (
                f"infra/{config}/{path.name} declares a {kind} that stores a parameter's value"
            )
        # A data block scoped inside a check block is stored in the state too.
        scoped = {
            rtype
            for check in parsed.get("check", [])
            for body in check.values()
            for block in body.get("data", [])
            for rtype in block
        }
        assert not scoped & _PARAMETER_VALUE_TYPES, (
            f"infra/{config}/{path.name} reads a parameter's value in a check block"
        )


@pytest.mark.parametrize("config", ["bootstrap", "live"])
def test_configuration_is_only_tf_files(config: str) -> None:
    """tofu also loads ``*.tf.json``, ``*.tofu`` and ``*.tofu.json``, and a ``.tofu``
    file replaces the ``.tf`` file of the same name. Every check here reads ``*.tf``."""
    others = [
        p.name
        for pattern in ("*.tf.json", "*.tofu", "*.tofu.json")
        for p in (INFRA / config).glob(pattern)
    ]
    assert others == []


def test_live_role_carries_exactly_its_inline_policies() -> None:
    """The role-side twin of the one-policy-per-user check. The apply role's
    ``iam:PutRolePolicy`` would apply any third inline policy on marketlake-instance."""
    policies = sorted(
        (_role_name(body["role"]), address.split(".")[1])
        for address, body in _resources("live").items()
        if address.split(".")[0] == "aws_iam_role_policy"
    )
    assert policies == [
        ("instance", "instance_config_read"),
        ("instance", "instance_s3"),
        ("instance", "instance_s3_read"),
    ]


def test_live_role_policy_names_are_distinct() -> None:
    """``PutRolePolicy`` keys an inline policy by its role and name, so two blocks with
    one name make the second apply overwrite the first. The S3 read half and the write
    half are the pair this protects (#686), and a mock provider plans both."""
    names = [
        (_role_name(body["role"]), body["name"])
        for address, body in _resources("live").items()
        if address.split(".")[0] == "aws_iam_role_policy"
    ]
    assert len(names) == len(set(names)), names
    assert ("instance", "backup-bucket") in names
    assert ("instance", "backup-bucket-read") in names


def test_token_writer_user_sets_only_its_name() -> None:
    """A path, tags or a permissions boundary changes what the apply role's grant on
    ``user/marketlake-token-writer`` must allow, and the first apply is refused."""
    body = _resources("live")["aws_iam_user.token_writer"]
    assert set(body) <= {"name", "lifecycle"}


def test_bootstrap_holds_only_known_resource_types() -> None:
    types = {address.split(".")[0] for address in _resources("bootstrap")}
    assert types == BOOTSTRAP_TYPES


def test_live_holds_only_known_resource_types() -> None:
    types = {address.split(".")[0] for address in _resources("live")}
    assert types == LIVE_TYPES


def test_live_roles_leave_their_policies_to_separate_resources() -> None:
    """``managed_policy_arns`` and ``inline_policy`` on a role manage its policies
    exclusively, so on every apply they detach a managed policy or delete an inline one
    that they do not list. ``managed_policy_arns`` would detach the SSM policy from
    ``marketlake-instance``, and the apply role's ``iam:DetachRolePolicy`` grant on that
    ARN lets the detach succeed silently. ``inline_policy`` would delete every inline
    policy it does not list, the S3 read half and the config read among them, and the
    gated ``backup-bucket`` write half once ``instance_s3_enabled`` is on.
    ``python-hcl2`` files a
    ``dynamic "inline_policy"`` block under ``dynamic``, so that key is read too."""
    roles = {a: body for a, body in _resources("live").items() if a.startswith("aws_iam_role.")}
    assert roles, "infra/live declares no role, so this check reads nothing"
    for address, body in roles.items():
        assert "managed_policy_arns" not in body, f"infra/live/{address} sets managed_policy_arns"
        assert "inline_policy" not in body, f"infra/live/{address} sets inline_policy"
        dynamic = [label for block in body.get("dynamic", []) for label in block]
        assert "inline_policy" not in dynamic, f"infra/live/{address} sets inline_policy"


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
        ("user", "marketlake-token-writer"),
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


# Each of these gives a user a policy that no aws_iam_user_policy block shows.
_USER_POLICY_ROUTES = {
    "aws_iam_user_policy_attachment",
    "aws_iam_policy_attachment",
    "aws_iam_user_group_membership",
    "aws_iam_group_membership",
}


def test_each_live_user_carries_exactly_one_inline_policy() -> None:
    """``tofu test`` compares each user's policy by its address, so a second policy on
    the same user passes it. That would give the token writer more than its one put, or
    ``marketlake-backup`` an SSM permission."""
    resources = _resources("live")
    assert not {address.split(".")[0] for address in resources} & _USER_POLICY_ROUTES

    users = sorted(a.split(".")[1] for a in resources if a.split(".")[0] == "aws_iam_user")
    assert users
    policies: list[str] = []
    for address, body in resources.items():
        if address.split(".")[0] not in ("aws_iam_user", "aws_iam_user_policy"):
            continue
        # One block must mean one user or one policy.
        assert "count" not in body and "for_each" not in body, address
        if address.startswith("aws_iam_user_policy."):
            match = re.fullmatch(r"\$\{aws_iam_user\.(\w+)\.(?:name|id)\}", body["user"])
            assert match, f"a user must be referenced through its resource, got {body['user']!r}"
            policies.append(match.group(1))
    assert sorted(policies) == users


def _parsed(config: str) -> list[dict[str, Any]]:
    parsed = []
    for path in sorted((INFRA / config).glob("*.tf")):
        with path.open() as f:
            parsed.append(hcl2.load(f, serialization_options=_OPTIONS))
    return parsed


def _data_sources(config: str) -> dict[str, dict[str, Any]]:
    """Every data block in one configuration, keyed ``type.name``."""
    return {
        f"{dtype}.{name}": body
        for parsed in _parsed(config)
        for block in parsed.get("data", [])
        for dtype, named in block.items()
        for name, body in named.items()
    }


def _locals(config: str) -> dict[str, Any]:
    found: dict[str, Any] = {}
    for parsed in _parsed(config):
        for block in parsed.get("locals", []):
            for name, value in block.items():
                assert name not in found, f"local.{name} is declared twice"
                found[name] = value
    return found


def test_vm_ignores_exactly_the_changes_that_would_replace_or_stop_it() -> None:
    """A new AMI replaces the instance, a changed ``user_data`` stops and starts it, and
    a stopped instance reads back with no public address, which plans a replacement of
    the instance and its root volume. ``instance_type`` stays out, so a resize still
    applies. #686 item 4 carries the reasoning."""
    lifecycles = _resources("live")["aws_instance.vm"].get("lifecycle", [])
    ignored = [entry for block in lifecycles for entry in block.get("ignore_changes", [])]
    assert sorted(ignored) == ["ami", "associate_public_ip_address", "user_data"]


def test_lake_volume_zone_is_one_literal_the_subnet_lookup_shares() -> None:
    """A variable would offer a zone change that ``prevent_destroy`` refuses, and the
    instance lands in the subnet's zone, so the volume and the subnet must name the same
    one. The precondition must read it too, or it checks a zone nothing uses."""
    zone = _locals("live")["zone"]
    assert isinstance(zone, str)
    assert re.fullmatch(r"us-east-1[a-z]", zone), zone
    volume = _resources("live")["aws_ebs_volume.lake"]
    subnet = _data_sources("live")["aws_subnet.default"]
    assert volume["availability_zone"] == "${local.zone}"
    assert subnet["availability_zone"] == "${local.zone}"
    assert subnet["default_for_az"] is True
    conditions = [
        check["condition"]
        for block in volume.get("lifecycle", [])
        for check in block.get("precondition", [])
    ]
    assert any("local.zone" in condition for condition in conditions), conditions


def _keys(value: Any) -> set[str]:
    """Every key in a parsed block, at any depth."""
    if isinstance(value, dict):
        return set(value) | {key for inner in value.values() for key in _keys(inner)}
    if isinstance(value, list):
        return {key for inner in value for key in _keys(inner)}
    return set()


def test_live_sets_neither_disable_api_stop_nor_kms_key_id() -> None:
    """``disable_api_stop`` makes ``stop_instance_before_detaching`` fail, and AWS
    stores a ``kms_key_id`` as an ARN, so an alias or a key id shows as a change on every
    plan. Nested blocks such as ``root_block_device`` are read too."""
    resources = _resources("live")
    for address in ("aws_instance.vm", "aws_ebs_volume.lake"):
        assert address in resources, f"infra/live/{address} is missing"
    for address, body in resources.items():
        found = _keys(body) & {"disable_api_stop", "kms_key_id"}
        assert not found, f"infra/live/{address} sets {sorted(found)}"


_TEMPLATE = INFRA / "live" / "user-data.sh.tftpl"


def test_shim_template_takes_only_the_owner_and_the_volume_id() -> None:
    """``user_data`` is readable by anyone who can describe the instance. The plan shows
    only the rendered text, so a third variable fed from a secret would pass every
    ``tofu test`` run that checks for the values the test happens to know. ``$${`` is a
    literal ``${`` in a template, so it is skipped."""
    text = _TEMPLATE.read_text()
    assert "%{" not in text, "the shim uses a template directive"
    variables = set(re.findall(r"(?<!\$)\$\{\s*([^}]*?)\s*\}", text))
    assert variables == {"owner", "lake_volume_id"}

    user_data = _resources("live")["aws_instance.vm"]["user_data"]
    match = re.fullmatch(
        r'\$\{templatefile\("\$\{path\.module\}/user-data\.sh\.tftpl", \{(.*)\}\)\}', user_data
    )
    assert match, f"user_data is not a templatefile() of the shim, got {user_data[:60]!r}"
    passed = set(re.findall(r"(\w+)\s*=", match.group(1)))
    assert passed == {"owner", "lake_volume_id"}
