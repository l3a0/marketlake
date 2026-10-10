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

  assert {
    condition = jsondecode(aws_iam_role.deploy.assume_role_policy).Statement == [{
      Effect    = "Allow"
      Principal = { Federated = "arn:aws:iam::000000000000:oidc-provider/token.actions.githubusercontent.com" }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          "token.actions.githubusercontent.com:sub" = "repo:l3a0@5200900/marketlake@1346754080:environment:deploy"
          "token.actions.githubusercontent.com:ref" = "refs/heads/main"
        }
      }
    }]
    error_message = "The deploy role's trust is not exactly the deploy environment subject on main with the sts audience."
  }

  # deploy.yml asks for an 18,000-second session, and AWS refuses a request longer than
  # the role allows.
  assert {
    condition     = aws_iam_role.deploy.max_session_duration == 18000
    error_message = "The deploy role's session limit is not 18,000 seconds, so the deploy job cannot assume it for its 240 minutes."
  }

  assert {
    condition     = aws_iam_role.deploy.name == "marketlake-deploy"
    error_message = "The deploy role is not named marketlake-deploy."
  }

  # Scheduler alone, and only for a schedule in this account's default group (#867). The
  # apply role cannot edit a trust once it exists, so it must be right at creation.
  assert {
    condition = jsondecode(aws_iam_role.scheduler.assume_role_policy).Statement == [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = {
        StringEquals = {
          "aws:SourceAccount" = "000000000000"
          "aws:SourceArn"     = "arn:aws:scheduler:us-east-1:000000000000:schedule-group/default"
        }
      }
    }]
    error_message = "The scheduler role's trust is not exactly Scheduler, for this account's default schedule group."
  }

  assert {
    condition     = aws_iam_role.scheduler.name == "marketlake-scheduler"
    error_message = "The scheduler role is not named marketlake-scheduler."
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
        jsondecode(aws_iam_role_policy.deploy.policy).Statement,
        jsondecode(aws_iam_role_policy.scheduler.policy).Statement,
        ) : s.Effect == "Allow" && (
        can(s.NotAction)
        || length(setintersection(toset(flatten([s.Action])), toset([
          "s3:PutBucketVersioning", "iam:DeleteUser", "iam:DeleteUserPolicy", "iam:*", "s3:*", "*",
          "ssm:ModifyDocumentPermission", "ssm:CancelCommand", "ssm:*",
        ]))) > 0
        || anytrue([for a in flatten([s.Action]) : strcontains(a, "*") && a != "ec2:*"])
      )
    ])
    error_message = "A role allows an action the issue forbids, or a wildcard other than ec2:*."
  }

  # The deploy role reaches the VM through one document. A send grant on any other
  # document, AWS-RunShellScript among them, would let any step in the deploy job run
  # any command as root on the VM.
  assert {
    condition = alltrue([
      for s in jsondecode(aws_iam_role_policy.deploy.policy).Statement : alltrue([
        for r in flatten([s.Resource]) :
        r == "arn:aws:ssm:us-east-1:000000000000:document/marketlake-deploy*"
        || (r == "arn:aws:ec2:us-east-1:000000000000:instance/*"
        && try(s.Condition, null) == { StringEquals = { "ssm:resourceTag/marketlake:host" = "capture" } })
      ])
      if contains(flatten([s.Action]), "ssm:SendCommand")
    ])
    error_message = "The deploy role may send a command to a document other than marketlake-deploy, or to an instance not tagged marketlake:host = capture."
  }

  assert {
    condition = alltrue([
      for s in jsondecode(aws_iam_role_policy.deploy.policy).Statement : s.Effect == "Allow"
    ])
    error_message = "The deploy role's inline policy holds a Deny, which the reviewed policy does not."
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

# The presence checks above say why each statement exists. These say that nothing else
# does, so a widened Resource, a loosened Condition or one added action fails here.
run "policies_are_exactly_the_reviewed_statements" {
  command = plan

  assert {
    condition = jsondecode(aws_iam_role_policy.plan.policy) == {
      Version = "2012-10-17"
      Statement = [
        {
          Sid      = "DenyBackupObjectReads"
          Effect   = "Deny"
          Action   = ["s3:GetObject*"]
          Resource = ["arn:aws:s3:::example-lake-backup/*"]
        },
        {
          Sid    = "DenyConsoleAndCommandOutput"
          Effect = "Deny"
          Action = [
            "ec2:GetConsoleOutput",
            "ec2:GetConsoleScreenshot",
            "ssm:ListCommands",
            "ssm:ListCommandInvocations",
            "ssm:GetCommandInvocation",
          ]
          Resource = ["*"]
        },
        {
          Sid    = "DenyConfigParameterReads"
          Effect = "Deny"
          Action = ["ssm:GetParameter", "ssm:GetParameters", "ssm:GetParameterHistory"]
          Resource = [
            "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config",
            "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config/*",
          ]
        },
        {
          Sid      = "DenyParameterPathReads"
          Effect   = "Deny"
          Action   = ["ssm:GetParametersByPath"]
          Resource = ["*"]
        },
      ]
    }
    error_message = "The plan role's inline policy is not exactly the reviewed four Denies."
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.apply.policy) == {
      Version = "2012-10-17"
      Statement = [
        {
          Sid      = "DenyBackupObjectReads"
          Effect   = "Deny"
          Action   = ["s3:GetObject*"]
          Resource = ["arn:aws:s3:::example-lake-backup/*"]
        },
        {
          Sid    = "DenyConsoleAndCommandOutput"
          Effect = "Deny"
          Action = [
            "ec2:GetConsoleOutput",
            "ec2:GetConsoleScreenshot",
            "ssm:ListCommands",
            "ssm:ListCommandInvocations",
            "ssm:GetCommandInvocation",
          ]
          Resource = ["*"]
        },
        {
          Sid    = "DenyConfigParameterReads"
          Effect = "Deny"
          Action = ["ssm:GetParameter", "ssm:GetParameters", "ssm:GetParameterHistory"]
          Resource = [
            "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config",
            "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config/*",
          ]
        },
        {
          Sid      = "DenyParameterPathReads"
          Effect   = "Deny"
          Action   = ["ssm:GetParametersByPath"]
          Resource = ["*"]
        },
        {
          Sid      = "DenyBucketDelete"
          Effect   = "Deny"
          Action   = ["s3:DeleteBucket"]
          Resource = ["*"]
        },
        {
          Sid    = "DenyCredentialsAndTrustEdits"
          Effect = "Deny"
          Action = [
            "iam:CreateAccessKey",
            "iam:CreateLoginProfile",
            "iam:CreateServiceSpecificCredential",
            "iam:UpdateAssumeRolePolicy",
          ]
          Resource = ["*"]
        },
        {
          Sid      = "DenyVolumeDelete"
          Effect   = "Deny"
          Action   = ["ec2:DeleteVolume"]
          Resource = ["*"]
        },
        {
          Sid      = "DenySnapshotAndImageSharing"
          Effect   = "Deny"
          Action   = ["ec2:ModifySnapshotAttribute", "ec2:ModifyImageAttribute"]
          Resource = ["*"]
        },
        {
          Sid    = "BackupBucketConfiguration"
          Effect = "Allow"
          Action = [
            "s3:PutLifecycleConfiguration",
            "s3:PutEncryptionConfiguration",
            "s3:PutBucketPublicAccessBlock",
          ]
          Resource = ["arn:aws:s3:::example-lake-backup"]
        },
        {
          Sid    = "LiveStateWrite"
          Effect = "Allow"
          Action = ["s3:PutObject", "s3:DeleteObject"]
          Resource = [
            "arn:aws:s3:::example-state/live/terraform.tfstate",
            "arn:aws:s3:::example-state/live/terraform.tfstate.tflock",
          ]
        },
        {
          Sid    = "InstanceRoleWrite"
          Effect = "Allow"
          Action = [
            "iam:CreateRole",
            "iam:DeleteRole",
            "iam:UpdateRole",
            "iam:UpdateRoleDescription",
            "iam:TagRole",
            "iam:UntagRole",
            "iam:PutRolePolicy",
            "iam:DeleteRolePolicy",
            "iam:PassRole",
          ]
          Resource = ["arn:aws:iam::000000000000:role/marketlake-instance"]
        },
        {
          Sid      = "InstanceRoleSsmAttachment"
          Effect   = "Allow"
          Action   = ["iam:AttachRolePolicy", "iam:DetachRolePolicy"]
          Resource = ["arn:aws:iam::000000000000:role/marketlake-instance"]
          Condition = {
            ArnEquals = { "iam:PolicyARN" = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore" }
          }
        },
        {
          Sid    = "InstanceProfileWrite"
          Effect = "Allow"
          Action = [
            "iam:CreateInstanceProfile",
            "iam:DeleteInstanceProfile",
            "iam:AddRoleToInstanceProfile",
            "iam:RemoveRoleFromInstanceProfile",
            "iam:TagInstanceProfile",
            "iam:UntagInstanceProfile",
          ]
          Resource = ["arn:aws:iam::000000000000:instance-profile/marketlake-instance"]
        },
        {
          Sid      = "CommandUserWrite"
          Effect   = "Allow"
          Action   = ["iam:CreateUser", "iam:PutUserPolicy"]
          Resource = ["arn:aws:iam::000000000000:user/marketlake-command"]
        },
        {
          Sid    = "CommandRolesWrite"
          Effect = "Allow"
          Action = ["iam:CreateRole", "iam:PutRolePolicy"]
          Resource = [
            "arn:aws:iam::000000000000:role/marketlake-backup",
            "arn:aws:iam::000000000000:role/marketlake-token-writer",
          ]
        },
        {
          Sid      = "Ec2InHomeRegion"
          Effect   = "Allow"
          Action   = ["ec2:*"]
          Resource = ["*"]
          Condition = {
            StringEquals = { "aws:RequestedRegion" = "us-east-1" }
          }
        },
        {
          Sid    = "DeployDocumentWrite"
          Effect = "Allow"
          Action = [
            "ssm:CreateDocument",
            "ssm:UpdateDocument",
            "ssm:UpdateDocumentDefaultVersion",
            "ssm:DeleteDocument",
          ]
          Resource = ["arn:aws:ssm:us-east-1:000000000000:document/marketlake-deploy*"]
        },
        {
          Sid    = "StartSchedulesWrite"
          Effect = "Allow"
          Action = [
            "scheduler:CreateSchedule",
            "scheduler:UpdateSchedule",
            "scheduler:DeleteSchedule",
          ]
          Resource = ["arn:aws:scheduler:us-east-1:000000000000:schedule/default/marketlake-*"]
        },
        {
          Sid      = "PassTheSchedulerRole"
          Effect   = "Allow"
          Action   = ["iam:PassRole"]
          Resource = ["arn:aws:iam::000000000000:role/marketlake-scheduler"]
          Condition = {
            StringEquals = { "iam:PassedToService" = "scheduler.amazonaws.com" }
          }
        },
      ]
    }
    error_message = "The apply role's inline policy is not exactly the reviewed Denies and Allows."
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.deploy.policy) == {
      Version = "2012-10-17"
      Statement = [
        {
          Sid      = "FindTheVm"
          Effect   = "Allow"
          Action   = ["ec2:DescribeInstances"]
          Resource = ["*"]
        },
        {
          Sid      = "SendToTheCaptureHost"
          Effect   = "Allow"
          Action   = ["ssm:SendCommand"]
          Resource = ["arn:aws:ec2:us-east-1:000000000000:instance/*"]
          Condition = {
            StringEquals = { "ssm:resourceTag/marketlake:host" = "capture" }
          }
        },
        {
          Sid      = "SendTheDeployDocument"
          Effect   = "Allow"
          Action   = ["ssm:SendCommand"]
          Resource = ["arn:aws:ssm:us-east-1:000000000000:document/marketlake-deploy*"]
        },
        {
          Sid      = "ReadTheDeployResult"
          Effect   = "Allow"
          Action   = ["ssm:GetCommandInvocation"]
          Resource = ["*"]
        },
      ]
    }
    error_message = "The deploy role's inline policy is not exactly the reviewed four Allows."
  }

  assert {
    condition     = aws_iam_role_policy.deploy.role == aws_iam_role.deploy.name
    error_message = "The deploy policy is not on marketlake-deploy."
  }

  # The case for keeping this role in the bootstrap rests on it holding this one grant
  # (#865). Any other action would be callable through Scheduler's universal target.
  assert {
    condition = jsondecode(aws_iam_role_policy.scheduler.policy) == {
      Version = "2012-10-17"
      Statement = [
        {
          Sid      = "StartTheCaptureHost"
          Effect   = "Allow"
          Action   = ["ec2:StartInstances"]
          Resource = ["arn:aws:ec2:us-east-1:000000000000:instance/*"]
          Condition = {
            StringEquals = { "aws:ResourceTag/marketlake:host" = "capture" }
          }
        },
      ]
    }
    error_message = "The scheduler role's inline policy is not exactly ec2:StartInstances on the instance tagged marketlake:host = capture."
  }

  assert {
    condition     = aws_iam_role_policy.scheduler.role == aws_iam_role.scheduler.name
    error_message = "The scheduler policy is not on marketlake-scheduler."
  }
}

run "state_bucket_and_oidc_provider" {
  command = plan

  assert {
    condition     = aws_s3_bucket_versioning.state.versioning_configuration[0].status == "Enabled"
    error_message = "The state bucket's versioning is not Enabled, so an overwritten state has no older copy."
  }

  assert {
    condition     = aws_iam_openid_connect_provider.github.url == "https://token.actions.githubusercontent.com"
    error_message = "The OIDC provider is not GitHub's, so no role's trust matches a token."
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

# An ARN or a name with a space would match an unanchored pattern, and "ab" is one
# character short of S3's minimum.
run "an_arn_is_not_a_bucket_name" {
  command = plan

  variables {
    state_bucket  = "arn:aws:s3:::example-state"
    backup_bucket = "arn:aws:s3:::example-state"
  }

  expect_failures = [var.state_bucket, var.backup_bucket]
}

run "a_two_character_bucket_name_fails_validation" {
  command = plan

  variables {
    state_bucket  = "ab"
    backup_bucket = "ab"
  }

  expect_failures = [var.state_bucket, var.backup_bucket]
}

run "a_bucket_name_with_a_space_fails_validation" {
  command = plan

  variables {
    state_bucket  = "has space"
    backup_bucket = "has space"
  }

  expect_failures = [var.state_bucket, var.backup_bucket]
}

# A Deny naming a hard-coded account id is a Deny on nobody's parameters.
run "config_denies_follow_the_callers_account" {
  command = plan

  override_data {
    target = data.aws_caller_identity.current
    values = {
      account_id = "111111111111"
    }
  }

  assert {
    condition = alltrue([
      for p in [aws_iam_role_policy.plan.policy, aws_iam_role_policy.apply.policy] :
      [for s in jsondecode(p).Statement : s.Resource if try(s.Sid, "") == "DenyConfigParameterReads"] == [[
        "arn:aws:ssm:us-east-1:111111111111:parameter/marketlake/config",
        "arn:aws:ssm:us-east-1:111111111111:parameter/marketlake/config/*",
      ]]
    ])
    error_message = "A config-parameter Deny does not name the caller's account."
  }

  # A grant naming a hard-coded account id is a grant on nobody's VM or document.
  assert {
    condition = [for s in jsondecode(aws_iam_role_policy.deploy.policy).Statement : s.Resource] == [
      ["*"],
      ["arn:aws:ec2:us-east-1:111111111111:instance/*"],
      ["arn:aws:ssm:us-east-1:111111111111:document/marketlake-deploy*"],
      ["*"],
    ]
    error_message = "A deploy-role grant does not name the caller's account."
  }

  assert {
    condition = [
      for s in jsondecode(aws_iam_role_policy.apply.policy).Statement : s.Resource
      if try(s.Sid, "") == "DeployDocumentWrite"
    ] == [["arn:aws:ssm:us-east-1:111111111111:document/marketlake-deploy*"]]
    error_message = "The apply role's document grant does not name the caller's account."
  }

  assert {
    condition     = jsondecode(aws_iam_role.deploy.assume_role_policy).Statement[0].Principal.Federated == "arn:aws:iam::111111111111:oidc-provider/token.actions.githubusercontent.com"
    error_message = "The deploy role's trust does not name the caller's OIDC provider."
  }

  assert {
    condition = [
      for s in jsondecode(aws_iam_role_policy.apply.policy).Statement : s.Resource
      if contains(["StartSchedulesWrite", "PassTheSchedulerRole"], try(s.Sid, ""))
      ] == [
      ["arn:aws:scheduler:us-east-1:111111111111:schedule/default/marketlake-*"],
      ["arn:aws:iam::111111111111:role/marketlake-scheduler"],
    ]
    error_message = "The apply role's schedule or PassRole grant does not name the caller's account."
  }

  assert {
    condition = [
      jsondecode(aws_iam_role.scheduler.assume_role_policy).Statement[0].Condition.StringEquals,
      jsondecode(aws_iam_role_policy.scheduler.policy).Statement[0].Resource,
      ] == [
      {
        "aws:SourceAccount" = "111111111111"
        "aws:SourceArn"     = "arn:aws:scheduler:us-east-1:111111111111:schedule-group/default"
      },
      ["arn:aws:ec2:us-east-1:111111111111:instance/*"],
    ]
    error_message = "The scheduler role's trust or grant does not name the caller's account."
  }
}
