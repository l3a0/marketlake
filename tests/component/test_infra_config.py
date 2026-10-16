"""Checks on ``infra/`` that ``tofu test`` cannot make, read from the ``.tf`` files.

``tofu test`` sees one configuration's plan, and twenty-one things are not in one.

1. ``prevent_destroy``. A test refuses destroy-mode plans, and ``tofu show -json`` omits
   ``lifecycle``, so removing the line leaves every ``tofu test`` run green.
2. Every policy attached to each of the bootstrap's four roles, the plan, apply,
   deploy and scheduler roles. An assert names the attachments it knows about, so a
   second attachment of the same type passes it. The plan role is trusted on every pull
   request from a branch here with no approval, so any write it gains lets an unreviewed
   branch change the account. The deploy role carries no ``ReadOnlyAccess``, so an
   attachment there would widen it past the one document it may send. Any grant the
   scheduler role gains can be called through Scheduler's universal target by a schedule
   the apply role writes (#865). No role may set a permissions boundary, which would cap
   it below its policy and fail its calls, or a path, which would move the ARN that
   infra/live, the PassRole grants and the workflows name.
3. Whether the live backend's key is the one the apply role may write. A mismatch
   passes every pull request check and fails the first apply after the merge.
4. The backend's own settings. Without ``use_lockfile`` two applies can write the state
   at once, and without ``encrypt`` it lands unencrypted.
5. Whether the IAM names the apply role may write in ``infra/bootstrap`` are the names
   ``infra/live`` declares. Each configuration's tests see only their own side. The
   scheduler role is the one exception, since the bootstrap declares it and the apply
   role may only pass it.
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
10. Whether every ``removed`` block forgets its resource without deleting it. A test
    plans against an empty state, so a ``removed`` block changes nothing there, and one
    that would delete a user passes every run.
11. Whether a role the laptop assumes sets anything beyond its name, its trust and its
    lifecycle. An assert can only name the arguments it knows about, so an added tag or
    session duration passes every ``tofu test`` run and is refused at the apply.
12. The VM's ``ignore_changes`` list and its ``depends_on``, neither of which a plan
    shows. Without an ``ignore_changes`` entry, a new AMI or a stop that drops the
    public address plans a replacement of the instance (#686). Without ``depends_on``,
    the first boot can run before the role's grants exist.
13. Whether two inline policies on one role share a name. ``PutRolePolicy`` on a shared
    name makes one overwrite the other, and a mock provider plans both.
14. Whether the lake volume's zone is one literal that the subnet lookup shares. A
    variable would offer a change ``prevent_destroy`` refuses, and the mock plans any
    zone.
15. Arguments that must stay absent. ``disable_api_stop`` makes the attachment's stop
    before a detach fail, and a ``kms_key_id`` reads back as an ARN, so every plan
    shows a change.
16. The shim template's variables. A third one could carry a value into ``user_data``,
    which anyone who can describe the instance reads, and the plan shows only the
    rendered text.
17. The ``depends_on`` on GitHub's OIDC provider of each role GitHub assumes, the plan,
    apply and deploy roles. The trust names the provider by a string, so nothing else
    orders the role after the provider on a first apply, and a plan shows no ordering.
18. Whether the role each start schedule passes is the scheduler role the bootstrap
    declares and the apply role may pass. A mismatch passes both configurations' tests
    and the plan, and fails the apply after the merge.
19. Whether each start schedule's name and group fit the apply role's grant and the
    scheduler role's trust. A mismatch fails the same way.
20. Whether the scheduler role's tag condition names the VM's own ``marketlake:host``
    tag and value. A mismatch applies cleanly, and every start then fails silently at
    07:30.
21. Whether every day the roster in ``lake.deploy_window`` runs has a start, early
    enough for the day's first unit and for marketlake-up's morning check, and late
    enough that the stop cannot power the VM off again before the day's span. The
    bounds read that check's timer from ``lake.control_plane`` and the stop's uptime
    grace from ``deploy/vm-stop.sh``, rather than from copies.

A ``module`` block would hide its resources from every check here, so neither
configuration may call one.

One check here a plan could make: the instance's tag keys. The instance serves its tags
through instance metadata, where EC2 refuses a key holding a ``/`` or a space, and the
config render reads the backup target there. ``infra.yml`` runs ``tofu test`` only when
the workflow or a file under ``infra/`` other than Markdown changes, and a mock provider
plans any key, so the rule sits here, where every pull request's required ``test`` job
runs it.

A second one states a rule where ``tofu test`` compares a literal: every ``iam:PassRole``
grant names the one service its role goes to (#867). The apply role may recreate
``marketlake-instance`` with any trust and policy, so a pass of it to Scheduler would let
one approved apply call ``ec2:DeleteVolume`` through a schedule. The bootstrap's
exact-policy run catches a dropped condition, and a pull request that edits the policy
and that run's literal together passes it.

These run in ``ci.yml``'s required ``test`` job, which has no OpenTofu. The parse is
``python-hcl2``'s, which keeps a function call such as ``jsonencode({...})`` as text, so
:func:`_jsonencode_argument` parses the call's argument as HCL on its own.
"""

from __future__ import annotations

import fnmatch
import re
from datetime import date, datetime, time, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import hcl2
import pytest

from lake import control_plane, deploy_window
from lake.calendar import MARKET_TZ

INFRA = Path(__file__).resolve().parents[2] / "infra"
VM_STOP = INFRA.parent / "deploy" / "vm-stop.sh"

_OPTIONS = hcl2.SerializationOptions(
    strip_string_quotes=True, explicit_blocks=False, with_comments=False
)

# Price 4 of issue #664: every resource whose loss would lose backups or stop them. The
# laptop's user, its two roles and their policies are here too (#737). CI cannot delete
# any of them, so a pull request that renames one fails at plan rather than at apply.
# The billing bucket holds every month of the raw cost export, which the owner keeps
# indefinitely, and the export itself would be deleted and recreated by any change but
# its query's, so both are here too (#912). This roster is the only check that the line
# is present.
PREVENT_DESTROY = [
    "bootstrap/aws_s3_bucket.state",
    "bootstrap/aws_s3_bucket_versioning.state",
    "live/aws_s3_bucket.backup",
    "live/aws_s3_bucket_versioning.backup",
    "live/aws_s3_bucket_server_side_encryption_configuration.backup",
    "live/aws_s3_bucket_public_access_block.backup",
    "live/aws_s3_bucket_lifecycle_configuration.backup",
    "live/aws_iam_user.command",
    "live/aws_iam_user_policy.command",
    "live/aws_iam_role.backup",
    "live/aws_iam_role_policy.backup",
    "live/aws_iam_role.token_writer",
    "live/aws_iam_role_policy.token_writer",
    # The lake volume holds every minute captured since the last nightly upload (#686).
    "live/aws_ebs_volume.lake",
    "live/aws_s3_bucket.billing",
    "live/aws_bcmdataexports_export.cost",
]

# The roles marketlake-command assumes, which the apply role may create and never delete.
COMMAND_ROLES = ["marketlake-backup", "marketlake-token-writer"]

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
    "aws_ssm_document",
    "aws_scheduler_schedule",
    "aws_s3_bucket_policy",
    "aws_bcmdataexports_export",
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
    ``iam:PutRolePolicy`` would apply any further inline policy on marketlake-instance,
    marketlake-backup or marketlake-token-writer."""
    policies = sorted(
        (_role_name(body["role"]), address.split(".")[1])
        for address, body in _resources("live").items()
        if address.split(".")[0] == "aws_iam_role_policy"
    )
    assert policies == [
        ("backup", "backup"),
        ("instance", "instance_config_read"),
        ("instance", "instance_cost_read"),
        ("instance", "instance_s3"),
        ("instance", "instance_s3_read"),
        ("token_writer", "token_writer"),
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


def test_command_user_sets_only_its_name() -> None:
    """A path, tags or a permissions boundary changes what the apply role's grant on
    ``user/marketlake-command`` must allow, and the first apply is refused."""
    body = _resources("live")["aws_iam_user.command"]
    assert set(body) <= {"name", "lifecycle"}


@pytest.mark.parametrize("address", ["aws_iam_role.backup", "aws_iam_role.token_writer"])
def test_command_role_sets_only_its_three_arguments(address: str) -> None:
    """A tag, a description or a ``max_session_duration`` needs an IAM action the apply
    role's grant on the role does not allow, such as ``iam:TagRole`` or
    ``iam:UpdateRole``, and the first apply is refused partway through."""
    body = _resources("live")[address]
    assert set(body) == {"name", "assume_role_policy", "lifecycle"}


@pytest.mark.parametrize("config", ["bootstrap", "live"])
def test_every_removed_block_keeps_the_resource(config: str) -> None:
    """A ``removed`` block without ``destroy = false`` plans a delete. The apply role
    may not delete a user or its policy, so that apply fails partway through, and a
    bare block forgets but warns."""
    for path in sorted((INFRA / config).glob("*.tf")):
        with path.open() as f:
            parsed = hcl2.load(f, serialization_options=_OPTIONS)
        for block in parsed.get("removed", []):
            lifecycles = block.get("lifecycle", [])
            assert [lifecycle.get("destroy") for lifecycle in lifecycles] == [False], (
                f"infra/{config}/{path.name}'s removed block for {block.get('from')} "
                "does not carry destroy = false"
            )


def test_rosters_cover_every_command_resource() -> None:
    """command.tf says all six of its resources carry prevent_destroy. The rosters are
    the only check on that, so a resource missing from them is checked by nothing."""
    with (INFRA / "live" / "command.tf").open() as f:
        parsed = hcl2.load(f, serialization_options=_OPTIONS)
    addresses = [
        f"{rtype}.{name}"
        for block in parsed["resource"]
        for rtype, named in block.items()
        for name in named
    ]
    assert len(addresses) == 6
    assert {f"live/{a}" for a in addresses} <= set(PREVENT_DESTROY)
    roles = sorted(
        named[name]["name"]
        for block in parsed["resource"]
        for rtype, named in block.items()
        if rtype == "aws_iam_role"
        for name in named
    )
    assert roles == sorted(COMMAND_ROLES)


def test_bootstrap_holds_only_known_resource_types() -> None:
    types = {address.split(".")[0] for address in _resources("bootstrap")}
    assert types == BOOTSTRAP_TYPES


def test_live_holds_only_known_resource_types() -> None:
    types = {address.split(".")[0] for address in _resources("live")}
    assert types == LIVE_TYPES


def test_live_holds_one_bucket_policy_and_one_export() -> None:
    """``LIVE_TYPES`` admits a type, not a count, and the exact-compare runs in
    ``live.tftest.hcl`` read ``aws_s3_bucket_policy.billing`` and
    ``aws_bcmdataexports_export.cost`` by name. A second bucket policy aimed at the billing
    bucket would apply under the apply role's ``s3:PutBucketPolicy`` and replace AWS's own
    policy, and a second export with no ``prevent_destroy`` would apply under
    ``CreateExport`` on ``export/*``. Neither named run would see either one."""
    addresses = set(_resources("live"))
    for rtype, only in (
        ("aws_s3_bucket_policy", "billing"),
        ("aws_bcmdataexports_export", "cost"),
    ):
        declared = {a for a in addresses if a.split(".")[0] == rtype}
        assert declared == {f"{rtype}.{only}"}, declared


def test_the_export_waits_for_the_billing_bucket_policy() -> None:
    """Creating the export checks its destination, and the bucket policy is what lets the
    service write there. Without the ordering a first apply can call ``CreateExport``
    before ``PutBucketPolicy`` lands. The mock provider plans the two in any order, so no
    ``tofu test`` run can see it, as ``test_vm_waits_for_the_ssm_attachment_and_the_config_read``
    says of the VM."""
    body = _resources("live")["aws_bcmdataexports_export.cost"]
    assert body.get("depends_on") == ["${aws_s3_bucket_policy.billing}"], body


def test_bootstrap_bucket_variables_have_no_default() -> None:
    """The bootstrap is applied by hand from ``bootstrap.tfvars``, so no workflow check
    reaches its variables. A default on a bucket's name would let a forgotten tfvars line
    plan clean, granting on and denying reads of a bucket that is not the real one."""
    found = []
    for path in sorted((INFRA / "bootstrap").glob("*.tf")):
        with path.open() as f:
            parsed = hcl2.load(f, serialization_options=_OPTIONS)
        for block in parsed.get("variable", []):
            for name, body in block.items():
                if name.endswith("_bucket"):
                    found.append(name)
                    assert "default" not in body, name
    assert sorted(found) == ["backup_bucket", "billing_bucket", "state_bucket"], found


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
    assert roles == ["apply", "deploy", "plan", "scheduler"]
    for role in roles:
        body = resources[f"aws_iam_role.{role}"]
        assert "managed_policy_arns" not in body and "inline_policy" not in body
        # A boundary caps the role below its policy, so a boundary that denies the one
        # action a role needs fails each call while every policy check here passes.
        assert "permissions_boundary" not in body, f"aws_iam_role.{role} sets a boundary"
        # A path moves the role's ARN, so infra/live's literal scheduler ARN, the apply
        # role's PassRole grants and the workflows' role ARNs would name no role.
        assert body.get("path", "/") == "/", f"aws_iam_role.{role} sets a path"

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
    assert sorted(inline) == ["apply", "deploy", "plan", "scheduler"]
    assert len(inline["apply"]) == 1
    assert len(inline["deploy"]) == 1
    assert len(inline["scheduler"]) == 1
    # The plan role holds one policy of Denies, and one Allow that reads the cost export's
    # definition, which every plan's refresh needs and ReadOnlyAccess lacks (#912). Any
    # other Allow would reach a pull request from an unreviewed branch.
    assert len(inline["plan"]) == 2
    denies = [p for p in inline["plan"] if {s["Effect"] for s in p} == {"Deny"}]
    allows = [p for p in inline["plan"] if p not in denies]
    assert len(denies) == 1 and denies[0]
    assert allows == [
        [
            {
                "Sid": "ReadTheCostExport",
                "Effect": "Allow",
                "Action": ["bcm-data-exports:GetExport", "bcm-data-exports:ListTagsForResource"],
                "Resource": [
                    "arn:aws:bcm-data-exports:us-east-1:${local.account_id}:export/*",
                ],
            }
        ]
    ]
    deploy_effects = [statement["Effect"] for statement in inline["deploy"][0]]
    assert deploy_effects and set(deploy_effects) == {"Allow"}
    # The case for keeping this role in the bootstrap rests on this one grant (#865).
    assert inline["scheduler"][0] == [
        {
            "Sid": "StartTheCaptureHost",
            "Effect": "Allow",
            "Action": ["ec2:StartInstances"],
            "Resource": ["arn:aws:ec2:us-east-1:${local.account_id}:instance/*"],
            "Condition": {"StringEquals": {"aws:ResourceTag/marketlake:host": "capture"}},
        }
    ]


# Each role an apply may pass, and the one service it may go to. The apply role may
# recreate marketlake-instance with any trust and policy, so a pass to Scheduler would let
# one approved apply call any action through a schedule, ec2:DeleteVolume on the lake
# volume included, where DenyVolumeDelete does not reach (#867).
_PASSES = {
    "arn:aws:iam::${local.account_id}:role/marketlake-instance": "ec2.amazonaws.com",
    "arn:aws:iam::${local.account_id}:role/marketlake-scheduler": "scheduler.amazonaws.com",
}


def test_every_pass_role_names_the_one_service_that_takes_the_role() -> None:
    """A statement that grants ``iam:PassRole`` grants nothing else, so its condition
    binds the pass alone, and names ``iam:PassedToService``. Each passable role goes to
    exactly one service."""
    passes: dict[str, str] = {}
    for address, body in _resources("bootstrap").items():
        if address.split(".")[0] != "aws_iam_role_policy":
            continue
        for statement in _jsonencode_argument(body["policy"])["Statement"]:
            if statement["Effect"] != "Allow" or "iam:PassRole" not in _actions(statement):
                continue
            assert _actions(statement) == ["iam:PassRole"], (address, statement.get("Sid"))
            condition = statement.get("Condition")
            assert condition is not None and list(condition) == ["StringEquals"], (
                f"{address}'s {statement.get('Sid')} passes a role to any service"
            )
            assert list(condition["StringEquals"]) == ["iam:PassedToService"]
            service = condition["StringEquals"]["iam:PassedToService"]
            for resource in _listed(statement["Resource"]):
                assert resource not in passes, f"{resource} is passed by two statements"
                passes[resource] = service
    assert passes == _PASSES


@pytest.mark.parametrize("role", ["apply", "deploy", "plan"])
def test_bootstrap_role_waits_for_the_oidc_provider(role: str) -> None:
    """Each trust names the provider through a string built from the account id, not a
    reference, so only ``depends_on`` orders the role after the provider on a first
    apply into a fresh account. A plan shows no ordering."""
    body = _resources("bootstrap")[f"aws_iam_role.{role}"]
    assert body.get("depends_on") == ["${aws_iam_openid_connect_provider.github}"], body


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


# The role the bootstrap declares for itself, which the apply role may pass to Scheduler
# and never write (#865). It is the one IAM name the apply role's grants carry that
# infra/live does not declare.
_PASSED_ONLY = ("role", "marketlake-scheduler")


def test_apply_role_grants_name_the_iam_resources_live_declares() -> None:
    """A rename on either side passes both configurations' own tests and fails the
    first apply after the merge, with an AccessDenied on the renamed resource. The
    scheduler role is excepted by name, and only while the bootstrap declares it and the
    apply role's every grant on it is ``iam:PassRole``."""
    policy = _jsonencode_argument(_resources("bootstrap")["aws_iam_role_policy.apply"]["policy"])
    granted = set()
    passed_only_actions = set()
    for statement in policy["Statement"]:
        if statement["Effect"] != "Allow":
            continue
        for resource in statement["Resource"]:
            match = re.fullmatch(r"arn:aws:iam::\$\{local\.account_id\}:([a-z-]+)/(.+)", resource)
            if match:
                granted.add((match.group(1), match.group(2)))
                if (match.group(1), match.group(2)) == _PASSED_ONLY:
                    passed_only_actions.update(_actions(statement))

    declared = {
        (kind, body["name"])
        for kind, rtype in _IAM_KINDS.items()
        for address, body in _resources("live").items()
        if address.split(".")[0] == rtype
    }
    bootstrap_roles = {
        ("role", body["name"])
        for address, body in _resources("bootstrap").items()
        if address.split(".")[0] == "aws_iam_role"
    }
    assert granted == {
        ("role", "marketlake-instance"),
        ("instance-profile", "marketlake-instance"),
        ("user", "marketlake-command"),
        ("role", "marketlake-backup"),
        ("role", "marketlake-token-writer"),
        _PASSED_ONLY,
    }
    assert _PASSED_ONLY in bootstrap_roles
    assert passed_only_actions == {"iam:PassRole"}
    assert declared == granted - {_PASSED_ONLY}


# The actions that would let an apply delete a command role or its policy, and so replace
# a role with a new trust or stop the nightly upload.
_ROLE_DELETES = ["iam:DeleteRole", "iam:DeleteRolePolicy"]


@pytest.mark.parametrize("role", COMMAND_ROLES)
@pytest.mark.parametrize("action", _ROLE_DELETES)
def test_no_allow_deletes_a_command_role(role: str, action: str) -> None:
    """The apply role is denied ``iam:UpdateAssumeRolePolicy``, so a trust is written
    only at ``CreateRole``. A grant that deletes the role would let a plan replace it,
    which writes whatever trust the pull request gave it. Matching is by wildcard on
    both sides, so ``iam:Delete*`` or ``role/marketlake-*`` counts as a grant, and a
    ``NotAction`` statement counts as granting everything it does not name."""
    arn = f"arn:aws:iam::${{local.account_id}}:role/{role}"
    policy = _jsonencode_argument(_resources("bootstrap")["aws_iam_role_policy.apply"]["policy"])
    for statement in policy["Statement"]:
        if statement["Effect"] != "Allow":
            continue
        if not any(fnmatch.fnmatchcase(arn, pattern) for pattern in _listed(statement["Resource"])):
            continue
        if "NotAction" in statement:
            granted = not any(
                fnmatch.fnmatchcase(action.lower(), pattern.lower())
                for pattern in _listed(statement["NotAction"])
            )
        else:
            granted = any(
                fnmatch.fnmatchcase(action.lower(), pattern.lower())
                for pattern in _actions(statement)
            )
        assert not granted, f"{statement.get('Sid')} allows {action} on role/{role}"


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
    the same user passes it. That would give ``marketlake-command`` more than its two
    assumes, and the laptop's key a direct grant that no role's session records."""
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


def test_vm_waits_for_the_ssm_attachment_and_the_config_read() -> None:
    """The instance names neither grant through an argument, so only ``depends_on``
    orders a combined apply. Without it the first boot's SSM agent backs off for up to
    an hour, and the config render is refused. A plan shows no ordering."""
    depends_on = _resources("live")["aws_instance.vm"].get("depends_on", [])
    assert sorted(depends_on) == [
        "${aws_iam_role_policy.instance_config_read}",
        "${aws_iam_role_policy_attachment.instance_ssm}",
    ]


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


# Letters, digits and ``+ - = . _ : @``. That leaves out ``/`` and the space, the two
# characters EC2 refuses in a tag key the metadata service serves.
_METADATA_TAG_KEY = re.compile(r"[A-Za-z0-9+\-=._:@]+")


def test_instance_tag_keys_are_ones_instance_metadata_can_serve() -> None:
    """With ``instance_metadata_tags`` enabled, EC2 refuses to launch or tag an instance
    whose tag key holds a ``/`` or a space, since the metadata service serves each key as
    a path. The config render reads ``marketlake:backup-target`` there, so a key that
    breaks the endpoint stops the first boot's render. A mock provider plans any key."""
    tags = _resources("live")["aws_instance.vm"]["tags"]
    assert "marketlake:backup-target" in tags
    assert "marketlake:cost-export" in tags
    for key in tags:
        assert _METADATA_TAG_KEY.fullmatch(key), f"the instance tag key {key!r} has a / or space"


# -- the start schedules (#867) -----------------------------------------------------

_SCHEDULER_ROLE_ARN = re.compile(r"arn:aws:iam::\$\{local\.account_id\}:role/(.+)")


def _schedules() -> dict[str, dict[str, Any]]:
    """Every ``aws_scheduler_schedule`` in infra/live. The pair is named, so a third
    schedule fails here until the checks below account for it."""
    schedules = {
        address: body
        for address, body in _resources("live").items()
        if address.split(".")[0] == "aws_scheduler_schedule"
    }
    assert sorted(schedules) == [
        "aws_scheduler_schedule.start_sunday",
        "aws_scheduler_schedule.start_weekday",
    ]
    return schedules


def _bootstrap_policy(address: str) -> dict[str, Any]:
    return _jsonencode_argument(_resources("bootstrap")[address]["policy"])


def test_start_schedules_pass_the_role_the_bootstrap_declares() -> None:
    """Live builds the role's ARN from a name, and the bootstrap declares the role and
    grants the pass. A rename on either side passes both configurations' tests and the
    plan, and the apply after the merge fails on ``iam:PassRole`` or on a role that does
    not exist."""
    name = _resources("bootstrap")["aws_iam_role.scheduler"]["name"]
    passed = [
        resource
        for statement in _bootstrap_policy("aws_iam_role_policy.apply")["Statement"]
        if statement["Effect"] == "Allow"
        and "iam:PassRole" in _actions(statement)
        and statement.get("Condition")
        == {"StringEquals": {"iam:PassedToService": "scheduler.amazonaws.com"}}
        for resource in _listed(statement["Resource"])
    ]
    assert passed == [f"arn:aws:iam::${{local.account_id}}:role/{name}"]
    for address, body in _schedules().items():
        [target] = body["target"]
        match = _SCHEDULER_ROLE_ARN.fullmatch(target["role_arn"])
        assert match, f"infra/live/{address}'s role_arn is not built from the account id"
        assert match.group(1) == name, f"infra/live/{address} passes {match.group(1)}"


def test_start_schedules_fit_the_apply_grant_and_the_trust() -> None:
    """The apply role may write only ``schedule/default/marketlake-*``, and the scheduler
    role's trust accepts only the default group. A schedule outside the prefix fails the
    apply after the merge, and so does one in another group."""
    writes = [
        statement
        for statement in _bootstrap_policy("aws_iam_role_policy.apply")["Statement"]
        if statement["Effect"] == "Allow" and "scheduler:CreateSchedule" in _actions(statement)
    ]
    assert len(writes) == 1
    assert sorted(_actions(writes[0])) == [
        "scheduler:CreateSchedule",
        "scheduler:DeleteSchedule",
        "scheduler:UpdateSchedule",
    ]
    trust = _jsonencode_argument(
        _resources("bootstrap")["aws_iam_role.scheduler"]["assume_role_policy"]
    )
    [statement] = trust["Statement"]
    source_arn = statement["Condition"]["StringEquals"]["aws:SourceArn"]
    for address, body in _schedules().items():
        group = body.get("group_name", "default")
        assert group == "default", f"infra/live/{address} sits in group {group}"
        arn = f"arn:aws:scheduler:us-east-1:${{local.account_id}}:schedule/{group}/{body['name']}"
        assert any(
            fnmatch.fnmatchcase(arn, pattern) for pattern in _listed(writes[0]["Resource"])
        ), f"the apply role may not write infra/live/{address}, {arn}"
        assert source_arn == (
            f"arn:aws:scheduler:us-east-1:${{local.account_id}}:schedule-group/{group}"
        ), f"the scheduler role's trust does not accept group {group}"


def test_scheduler_role_starts_the_vm_by_its_own_tag() -> None:
    """The role's condition names a tag, and the VM carries tags. A key or value that
    differs applies cleanly in both configurations, and every 07:30 start is then
    refused with nothing but the morning check to say so."""
    tags = _resources("live")["aws_instance.vm"]["tags"]
    starts = [
        statement
        for statement in _bootstrap_policy("aws_iam_role_policy.scheduler")["Statement"]
        if "ec2:StartInstances" in _actions(statement)
    ]
    assert len(starts) == 1
    assert starts[0]["Condition"] == {
        "StringEquals": {"aws:ResourceTag/marketlake:host": tags["marketlake:host"]}
    }


# The three bounds below come from #865's item 7.8. The roster and MARGIN come from
# lake.deploy_window. The other two numbers belong to the stop that #868 built, and the
# bounds read both from where the VM reads them rather than from a copy (#871).
#
# marketlake-up's timer, the morning check that pages when the VM is not up, is a unit
# lake.control_plane renders, so the bounds read its time and days from that unit. Every
# host renders the same schedule, so this host's paths are placeholders.
_ANY_HOST = control_plane.SystemdHost(python="/p/python", owner="o", home="/h", project_dir="/h/m")

# The stop leaves a VM alone for its first stretch of uptime. marketlake-stop.service runs
# deploy/vm-stop.sh from the checkout and nothing renders it, so the bounds parse the
# script's one assignment. The one form they can read is a line that assigns a number and
# nothing else, written without a leading zero, since bash reads one as octal. Any other
# form fails here rather than being read wrongly, and so does any second line that names
# the variable other than as a $ read. That includes a bare name inside (( )), which
# only reads it, so the parse refuses more than it must. A comment line is skipped.
_UPTIME_FORM = "UPTIME_SECONDS=<digits>"
# [0-9] rather than \d, which also matches digits bash does not read, such as Arabic-Indic.
_UPTIME_ASSIGNMENT = re.compile(r"UPTIME_SECONDS=(0|[1-9][0-9]*)")
# A $ read, as $UPTIME_SECONDS or ${UPTIME_SECONDS}, sets nothing.
_UPTIME_READ = re.compile(r"\$UPTIME_SECONDS\b|\$\{UPTIME_SECONDS\}")
_UPTIME_NAME = re.compile(r"\bUPTIME_SECONDS\b")

# The time a weekday start allows for the VM to boot from stopped and its units to come up
# before marketlake-up checks it. No boot from stopped has been timed on this VM yet, so
# this is an allowance with room to spare, not a measurement.
BOOT_ALLOWANCE = timedelta(minutes=5)

# The least time a start keeps before the earliest roster unit of its day, so the VM has
# booted and mounted the lake volume before the first job runs (#865).
START_LEAD = timedelta(minutes=20)

# Scheduler numbers the days SUN 1 to SAT 7. Python numbers them MON 0 to SUN 6.
_SCHEDULER_DAYS = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"]

# The one cron form the bounds can read: a minute, an hour, any day of any month, one
# day or a range of days, and any year. Any other form fails here rather than being
# read wrongly.
_CRON = re.compile(r"cron\((\d{1,2}) (\d{1,2}) \? \* ([A-Z]{3})(?:-([A-Z]{3}))? \*\)")


def _cron_start(expression: str) -> tuple[time, set[int]]:
    """The wall-clock time and the Python weekdays a schedule's cron fires on."""
    match = _CRON.fullmatch(expression)
    assert match, f"the bounds cannot read {expression!r}"
    minute, hour, first, last = match.groups()
    first_index = _SCHEDULER_DAYS.index(first)
    last_index = _SCHEDULER_DAYS.index(last or first)
    assert first_index <= last_index, f"{expression!r} wraps past Saturday"
    days = {(index - 1) % 7 for index in range(first_index, last_index + 1)}
    return time(int(hour), int(minute)), days


def _uptime_grace(script: str) -> timedelta:
    """The stop's uptime grace, read from the one line of ``script`` that sets it."""
    naming = [
        line
        for line in script.splitlines()
        if not line.lstrip().startswith("#") and _UPTIME_NAME.search(_UPTIME_READ.sub("", line))
    ]
    assert len(naming) == 1, (
        f"the bounds read one line {_UPTIME_FORM}, and {len(naming)} lines name "
        f"UPTIME_SECONDS: {naming!r}"
    )
    match = _UPTIME_ASSIGNMENT.fullmatch(naming[0])
    assert match, f"the bounds read only a line {_UPTIME_FORM}, not {naming[0]!r}"
    return timedelta(seconds=int(match.group(1)))


# Five weeks, which cross the end of daylight saving time on 2026-11-01 and the early
# close on 2026-11-27.
_BOUND_DAYS = [date(2026, 10, 26) + timedelta(days=offset) for offset in range(35)]


def _check_start(address: str, at: time, days: set[int], stop_script: Path) -> int:
    """Check a start at ``at`` on ``days`` against the three bounds on every day of
    ``_BOUND_DAYS`` it fires, and return how many days it checked. Each start must

    1. come at least ``START_LEAD`` before the earliest roster unit of its day,
    2. on a day marketlake-up checks the VM, come more than ``BOOT_ALLOWANCE`` before
       that check, at the time and on the days ``control_plane.vm_up_job`` renders, and
    3. end the stop's uptime grace, read from ``stop_script``, at a moment the stop's
       own rule, ``deploy_window.decide``, refuses, so the window check already holds
       the VM up when the grace lets go.

    The third bound asks ``decide`` itself rather than restating its rule. ``decide``
    still allows a stop at exactly ``MARGIN`` before a span, 04:30 on a weekday and
    16:00 on a Sunday, so a weekday start at 03:30 or a Sunday start at 15:00 with an
    hour's grace would pass the first two bounds and power off before the session, with
    nothing to start the VM again.
    """
    vm_up = control_plane.vm_up_job(_ANY_HOST).schedule
    assert vm_up is not None, "marketlake-up has no timer, so bound 2 reads nothing"
    grace = _uptime_grace(stop_script.read_text())
    units = deploy_window.default_units()
    checked = 0
    for day in _BOUND_DAYS:
        if day.weekday() not in days:
            continue
        start = datetime.combine(day, at, tzinfo=MARKET_TZ)
        todays = [
            unit.schedule.at.on(day)
            for unit in units
            if unit.schedule is not None and day.weekday() in unit.schedule.days
        ]
        assert todays, f"{address} starts the VM on {day}, when no roster unit runs"
        assert start <= min(todays) - START_LEAD, (
            f"{address} starts at {start:%H:%M} on {day}, less than "
            f"{START_LEAD} before the first unit at {min(todays):%H:%M}"
        )
        if day.weekday() in vm_up.days:
            assert start + BOOT_ALLOWANCE < vm_up.at.on(day), (
                f"{address} starts at {start:%H:%M} on {day}, which leaves less than "
                f"{BOOT_ALLOWANCE} to boot before marketlake-up at {vm_up.at.on(day):%H:%M}"
            )
        released = start + grace
        assert deploy_window.decide(released, units).code == deploy_window.REFUSED, (
            f"{address}'s grace ends at {released:%H:%M} on {day}, when deploy_window "
            "still allows a stop"
        )
        checked += 1
    return checked


@pytest.mark.parametrize(
    "address", ["aws_scheduler_schedule.start_weekday", "aws_scheduler_schedule.start_sunday"]
)
def test_start_schedule_fits_the_roster(address: str) -> None:
    """Moving a job or a start fails here rather than at the open. ``_check_start``
    names the three bounds each start must meet."""
    body = _schedules()[address]
    assert body["schedule_expression_timezone"] == str(MARKET_TZ)
    at, days = _cron_start(body["schedule_expression"])
    checked = _check_start(address, at, days, VM_STOP)
    assert checked >= 5, f"{address} fired on {checked} of the days checked"


_WEEKDAYS = {0, 1, 2, 3, 4}
_BOUND_2_FAILS = "to boot before marketlake-up"
_BOUND_3_FAILS = "when deploy_window still allows a stop"


def _stub_vm_up(
    monkeypatch: pytest.MonkeyPatch, at: control_plane.WallClockTime, days: tuple[int, ...]
) -> None:
    """Make ``control_plane.vm_up_job`` render a timer at ``at`` on ``days``."""
    schedule = control_plane.Schedule(at, days)
    monkeypatch.setattr(control_plane, "vm_up_job", lambda host: SimpleNamespace(schedule=schedule))


def _script(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "vm-stop.sh"
    path.write_text(text)
    return path


def test_bound_2_fails_a_start_that_leaves_too_little_to_boot(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A 07:30 start leaves exactly ``BOOT_ALLOWANCE`` before a 07:35 check, which bound 2
    refuses, and one minute more before a 07:36 check, which it allows."""
    script = _script(tmp_path, "UPTIME_SECONDS=3600\n")
    _stub_vm_up(monkeypatch, control_plane.WallClockTime(7, 35), (0, 1, 2, 3, 4))
    with pytest.raises(AssertionError, match=_BOUND_2_FAILS):
        _check_start("test", time(7, 30), _WEEKDAYS, script)
    _stub_vm_up(monkeypatch, control_plane.WallClockTime(7, 36), (0, 1, 2, 3, 4))
    assert _check_start("test", time(7, 30), _WEEKDAYS, script) == 25


def test_bound_2_reads_the_days_of_the_timer(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A timer that also fires on Sunday puts the Sunday evening start under bound 2."""
    script = _script(tmp_path, "UPTIME_SECONDS=3600\n")
    _stub_vm_up(monkeypatch, control_plane.WallClockTime(7, 40), (0, 1, 2, 3, 4, 6))
    with pytest.raises(AssertionError, match=_BOUND_2_FAILS):
        _check_start("test", time(19, 30), {6}, script)


def test_bound_3_fails_a_grace_that_ends_while_a_stop_is_allowed(tmp_path: Path) -> None:
    """A 04:00 start with a 20-minute grace lets go at 04:20, before 04:30, the last
    moment ``decide`` allows a stop before the session. A 30-minute grace lets go at
    04:30 itself, which ``decide`` still allows. An hour lets go at 05:00."""
    for seconds in [1200, 1800]:
        short = _script(tmp_path, f"UPTIME_SECONDS={seconds}\n")
        with pytest.raises(AssertionError, match=_BOUND_3_FAILS):
            _check_start("test", time(4, 0), _WEEKDAYS, short)
    hour = _script(tmp_path, "UPTIME_SECONDS=3600\n")
    assert _check_start("test", time(4, 0), _WEEKDAYS, hour) == 25


def test_uptime_grace_reads_the_assignment_and_skips_the_reads() -> None:
    """Bound 3 has hours of slack on both live starts, so no grace of zero or more turns it
    red, and a wrong read would pass it. The read is checked here instead, on a value the
    live script does not hold."""
    script = (
        "#!/bin/bash\n"
        "# UPTIME_SECONDS=7200 in a comment sets nothing.\n"
        "#UPTIME_SECONDS=7200\n"
        "  # UPTIME_SECONDS=7200 in an indented comment sets nothing.\n"
        "MAX_UPTIME_SECONDS=86400\n"
        "set -euo pipefail\n"
        "\n"
        "UPTIME_SECONDS=1800\n"
        "POWEROFF_WAIT=240\n"
        'if [[ "$UP" -lt "$UPTIME_SECONDS" ]]; then\n'
        '  refuse "2 (uptime)" "up $UP seconds, less than ${UPTIME_SECONDS}"\n'
        "fi\n"
    )
    assert _uptime_grace(script) == timedelta(minutes=30)
    assert _uptime_grace("UPTIME_SECONDS=0\n") == timedelta(0)
    # Not a whole number of minutes, so a read that rounds to minutes fails here.
    assert _uptime_grace("UPTIME_SECONDS=3659\n") == timedelta(seconds=3659)


@pytest.mark.parametrize(
    "script",
    [
        "set -euo pipefail\nPOWEROFF_WAIT=240\n",
        "readonly UPTIME_SECONDS=3600\n",
        "export UPTIME_SECONDS=3600\n",
        "declare -r UPTIME_SECONDS=3600\n",
        "  UPTIME_SECONDS=3600\n",
        'UPTIME_SECONDS="3600"\n',
        'UPTIME_SECONDS="${X:-3600}"\n',
        ': "${UPTIME_SECONDS:=3600}"\n',
        "UPTIME_SECONDS=$((60*60))\n",
        "UPTIME_SECONDS=03600\n",
        # A 3 followed by Arabic-Indic digits, which int() reads as 3600 and bash does not.
        "UPTIME_SECONDS=3٦٠٠\n",
        "UPTIME_SECONDS=3600  # an hour\n",
        "UPTIME_SECONDS=3600\nUPTIME_SECONDS=1800\n",
        "UPTIME_SECONDS=3600\nUPTIME_SECONDS=60  # for a test\n",
        'UPTIME_SECONDS=3600\n[[ -n "${FAST:-}" ]] && UPTIME_SECONDS=60\n',
        "UPTIME_SECONDS=3600\nreadonly UPTIME_SECONDS\n",
    ],
)
def test_uptime_grace_refuses_any_other_form(script: str) -> None:
    """A form the parse cannot read fails, naming the one form it reads, rather than
    handing bound 3 a number the script does not use."""
    with pytest.raises(AssertionError, match=re.escape(_UPTIME_FORM)):
        _uptime_grace(script)


def test_uptime_grace_reads_the_stop_script() -> None:
    """The live script is written in the one form the parse reads."""
    assert isinstance(_uptime_grace(VM_STOP.read_text()), timedelta)


def test_every_day_the_roster_runs_has_a_start() -> None:
    """The bounds above check each start on the days it fires, so a day dropped from a
    cron passes them. Once the VM stops itself, a roster day with no start is a day the
    VM never comes up, holidays included."""
    started: set[int] = set()
    for body in _schedules().values():
        started |= _cron_start(body["schedule_expression"])[1]
    roster_days = {
        day for unit in deploy_window.default_units() if unit.schedule for day in unit.schedule.days
    }
    assert roster_days, "the roster has no scheduled unit, so this check reads nothing"
    assert roster_days <= started, f"no schedule starts the VM on {sorted(roster_days - started)}"
