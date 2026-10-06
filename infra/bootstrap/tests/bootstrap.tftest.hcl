# Plan-mode checks on the bootstrap roles, against literals. Every run plans, because
# test cleanup cannot destroy a prevent_destroy resource and still reports success.
# prevent_destroy itself, and each role's exact set of policies, are checked by
# tests/component/test_infra_config.py, which reads the .tf files.

mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "000000000000"
    }
  }
}

variables {
  state_bucket   = "example-state"
  backup_bucket  = "example-lake-backup"
  adopt_existing = false
}

run "trust_policies_match_the_oidc_subjects" {
  command = plan

  assert {
    condition = jsondecode(aws_iam_role.plan.assume_role_policy).Statement == [{
      Effect    = "Allow"
      Principal = { Federated = "arn:aws:iam::000000000000:oidc-provider/token.actions.githubusercontent.com" }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          "token.actions.githubusercontent.com:sub" = "repo:l3a0@5200900/marketlake@1346754080:pull_request"
        }
      }
    }]
    error_message = "The plan role's trust is not exactly the pull_request subject with the sts audience."
  }

  assert {
    condition = jsondecode(aws_iam_role.apply.assume_role_policy).Statement == [{
      Effect    = "Allow"
      Principal = { Federated = "arn:aws:iam::000000000000:oidc-provider/token.actions.githubusercontent.com" }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          "token.actions.githubusercontent.com:sub" = "repo:l3a0@5200900/marketlake@1346754080:environment:infra"
          "token.actions.githubusercontent.com:ref" = "refs/heads/main"
        }
      }
    }]
    error_message = "The apply role's trust is not exactly the infra environment subject on main with the sts audience."
  }
}

run "every_deny_is_present" {
  command = plan

  # Each pair is an action and the resource its Deny must cover. A Deny with a
  # Condition does not count, because a condition can switch it off.
  assert {
    condition = alltrue([
      for pair in [
        ["s3:GetObject*", "arn:aws:s3:::example-lake-backup/*"],
        ["ec2:GetConsoleOutput", "*"],
        ["ec2:GetConsoleScreenshot", "*"],
        ["ssm:ListCommands", "*"],
        ["ssm:ListCommandInvocations", "*"],
        ["ssm:GetCommandInvocation", "*"],
        ["ssm:GetParameter", "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config"],
        ["ssm:GetParameter", "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config/*"],
        ["ssm:GetParameters", "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config"],
        ["ssm:GetParameters", "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config/*"],
        ["ssm:GetParameterHistory", "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config"],
        ["ssm:GetParameterHistory", "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config/*"],
        ["ssm:GetParametersByPath", "*"],
        ] : anytrue([
          for s in jsondecode(aws_iam_role_policy.plan.policy).Statement :
          s.Effect == "Deny" && try(s.Condition, null) == null
          && contains(flatten([s.Action]), pair[0]) && contains(flatten([s.Resource]), pair[1])
      ])
    ])
    error_message = "The plan role is missing a Deny on backup objects, console output, Run Command output or config parameters."
  }

  assert {
    condition = alltrue([
      for pair in [
        ["s3:GetObject*", "arn:aws:s3:::example-lake-backup/*"],
        ["ec2:GetConsoleOutput", "*"],
        ["ec2:GetConsoleScreenshot", "*"],
        ["ssm:ListCommands", "*"],
        ["ssm:ListCommandInvocations", "*"],
        ["ssm:GetCommandInvocation", "*"],
        ["ssm:GetParameter", "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config"],
        ["ssm:GetParameter", "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config/*"],
        ["ssm:GetParameters", "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config"],
        ["ssm:GetParameters", "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config/*"],
        ["ssm:GetParameterHistory", "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config"],
        ["ssm:GetParameterHistory", "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config/*"],
        ["ssm:GetParametersByPath", "*"],
        ["s3:DeleteBucket", "*"],
        ["iam:CreateAccessKey", "*"],
        ["iam:CreateLoginProfile", "*"],
        ["iam:CreateServiceSpecificCredential", "*"],
        ["iam:UpdateAssumeRolePolicy", "*"],
        ["ec2:DeleteVolume", "*"],
        ["ec2:ModifySnapshotAttribute", "*"],
        ["ec2:ModifyImageAttribute", "*"],
        ] : anytrue([
          for s in jsondecode(aws_iam_role_policy.apply.policy).Statement :
          s.Effect == "Deny" && try(s.Condition, null) == null
          && contains(flatten([s.Action]), pair[0]) && contains(flatten([s.Resource]), pair[1])
      ])
    ])
    error_message = "The apply role is missing one of its Denies."
  }
}

run "no_allow_grants_a_forbidden_action" {
  command = plan

  # The named actions are forbidden outright. Any other wildcard is forbidden too,
  # because `s3:Put*` grants s3:PutBucketVersioning without naming it. `ec2:*` is the
  # one wildcard granted, conditioned on the home region.
  assert {
    condition = !anytrue([
      for s in concat(
        jsondecode(aws_iam_role_policy.plan.policy).Statement,
        jsondecode(aws_iam_role_policy.apply.policy).Statement,
        ) : s.Effect == "Allow" && (
        can(s.NotAction)
        || length(setintersection(toset(flatten([s.Action])), toset([
          "s3:PutBucketVersioning", "iam:DeleteUser", "iam:DeleteUserPolicy", "iam:*", "s3:*", "*",
        ]))) > 0
        || anytrue([for a in flatten([s.Action]) : strcontains(a, "*") && a != "ec2:*"])
      )
    ])
    error_message = "A role allows an action the issue forbids, or a wildcard other than ec2:*."
  }

  assert {
    condition = alltrue([
      for s in jsondecode(aws_iam_role_policy.apply.policy).Statement :
      try(s.Condition, null) == { StringEquals = { "aws:RequestedRegion" = "us-east-1" } }
      if s.Effect == "Allow" && contains(flatten([s.Action]), "ec2:*")
    ])
    error_message = "The apply role's ec2:* is not limited to us-east-1."
  }

  assert {
    condition = alltrue([
      for s in jsondecode(aws_iam_role_policy.plan.policy).Statement : s.Effect == "Deny"
    ])
    error_message = "The plan role's inline policy allows something. Its writes would reach an unreviewed branch."
  }
}

run "empty_bucket_names_fail_validation" {
  command = plan

  variables {
    state_bucket  = ""
    backup_bucket = ""
  }

  expect_failures = [var.state_bucket, var.backup_bucket]
}
