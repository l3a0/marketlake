# Two roles, each assumed through GitHub's OIDC provider. Every policy is a
# jsonencode() literal rather than an aws_iam_policy_document, because under tofu
# test's mock provider that data source returns a random string and no test could read
# the document. The statements are written out in full in both roles, rather than
# shared through a local, so tests/component/test_infra_config.py can read them from
# the parse.

locals {
  # GitHub writes this repository's OIDC subjects with its owner and repository ids,
  # so `repo:l3a0/marketlake:*` would match nothing.
  github_subject_prefix = "repo:l3a0@5200900/marketlake@1346754080"
}

# -- the plan role ------------------------------------------------------------------

# Trusted on every pull request from a branch of this repository, with no approval, so
# it reads and never writes.
resource "aws_iam_role" "plan" {
  name = "marketlake-plan"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = local.github_oidc_provider_arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          "token.actions.githubusercontent.com:sub" = "${local.github_subject_prefix}:pull_request"
        }
      }
    }]
  })

  depends_on = [aws_iam_openid_connect_provider.github]
}

resource "aws_iam_role_policy_attachment" "plan_read_only" {
  role       = aws_iam_role.plan.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

# A pull request runs its own copy of the workflow and could print any of these into
# public logs. A refresh never needs them.
resource "aws_iam_role_policy" "plan" {
  name = "deny-sensitive-reads"
  role = aws_iam_role.plan.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "DenyBackupObjectReads"
        Effect   = "Deny"
        Action   = ["s3:GetObject*"]
        Resource = ["arn:aws:s3:::${var.backup_bucket}/*"]
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
        # ReadOnlyAccess grants ssm:Get*, and the VM's config.yaml secrets sit under
        # this path as SecureString parameters (#699).
        Sid    = "DenyConfigParameterReads"
        Effect = "Deny"
        Action = [
          "ssm:GetParameter",
          "ssm:GetParameters",
          "ssm:GetParameterHistory",
        ]
        Resource = [
          "arn:aws:ssm:us-east-1:${local.account_id}:parameter/marketlake/config",
          "arn:aws:ssm:us-east-1:${local.account_id}:parameter/marketlake/config/*",
        ]
      },
      {
        # A recursive read of any ancestor path returns every parameter below it, and a
        # Deny on the child does not stop it, so no path is readable at all.
        Sid      = "DenyParameterPathReads"
        Effect   = "Deny"
        Action   = ["ssm:GetParametersByPath"]
        Resource = ["*"]
      },
    ]
  })
}

# -- the apply role -----------------------------------------------------------------

# Trusted in two environments, and only on `main`. `infra` needs the owner's approval.
# `infra-auto` has no reviewers, and its job applies only a plan that
# infra/ci/classify.py accepts, as docs/design.md's "Infrastructure, defined" says. The
# ref check puts a branch check in AWS beside each environment's own.
resource "aws_iam_role" "apply" {
  name = "marketlake-apply"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = local.github_oidc_provider_arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          "token.actions.githubusercontent.com:sub" = [
            "${local.github_subject_prefix}:environment:infra",
            "${local.github_subject_prefix}:environment:infra-auto",
          ]
          "token.actions.githubusercontent.com:ref" = "refs/heads/main"
        }
      }
    }]
  })

  depends_on = [aws_iam_openid_connect_provider.github]
}

resource "aws_iam_role_policy_attachment" "apply_read_only" {
  role       = aws_iam_role.apply.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

# Narrow writes on what infra/live/ manages. Every read comes from ReadOnlyAccess.
resource "aws_iam_role_policy" "apply" {
  name = "apply-infra-live"
  role = aws_iam_role.apply.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "DenyBackupObjectReads"
        Effect   = "Deny"
        Action   = ["s3:GetObject*"]
        Resource = ["arn:aws:s3:::${var.backup_bucket}/*"]
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
        # ReadOnlyAccess grants ssm:Get*, and the VM's config.yaml secrets sit under
        # this path as SecureString parameters (#699).
        Sid    = "DenyConfigParameterReads"
        Effect = "Deny"
        Action = [
          "ssm:GetParameter",
          "ssm:GetParameters",
          "ssm:GetParameterHistory",
        ]
        Resource = [
          "arn:aws:ssm:us-east-1:${local.account_id}:parameter/marketlake/config",
          "arn:aws:ssm:us-east-1:${local.account_id}:parameter/marketlake/config/*",
        ]
      },
      {
        # A recursive read of any ancestor path returns every parameter below it, and a
        # Deny on the child does not stop it, so no path is readable at all.
        Sid      = "DenyParameterPathReads"
        Effect   = "Deny"
        Action   = ["ssm:GetParametersByPath"]
        Resource = ["*"]
      },
      {
        # A pull request that destroys a resource can also delete its prevent_destroy.
        Sid      = "DenyBucketDelete"
        Effect   = "Deny"
        Action   = ["s3:DeleteBucket"]
        Resource = ["*"]
      },
      {
        # An in-place change can neither mint a long-lived credential nor rewrite a
        # trust policy.
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
        # The lake volume holds every minute captured since the last nightly upload,
        # and no legitimate plan deletes it.
        Sid      = "DenyVolumeDelete"
        Effect   = "Deny"
        Action   = ["ec2:DeleteVolume"]
        Resource = ["*"]
      },
      {
        # Either would publish the VM's volume snapshots or images, and no plan needs
        # them.
        Sid    = "DenySnapshotAndImageSharing"
        Effect = "Deny"
        Action = [
          "ec2:ModifySnapshotAttribute",
          "ec2:ModifyImageAttribute",
        ]
        Resource = ["*"]
      },
      {
        # No object writes, and no s3:PutBucketVersioning, which is the only way to
        # suspend versioning.
        Sid    = "BackupBucketConfiguration"
        Effect = "Allow"
        Action = [
          "s3:PutLifecycleConfiguration",
          "s3:PutEncryptionConfiguration",
          "s3:PutBucketPublicAccessBlock",
        ]
        Resource = ["arn:aws:s3:::${var.backup_bucket}"]
      },
      {
        # The live state and its lock file, and nothing else in the state bucket.
        Sid    = "LiveStateWrite"
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:DeleteObject"]
        Resource = [
          "arn:aws:s3:::${var.state_bucket}/live/terraform.tfstate",
          "arn:aws:s3:::${var.state_bucket}/live/terraform.tfstate.tflock",
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
        Resource = ["arn:aws:iam::${local.account_id}:role/marketlake-instance"]
      },
      {
        Sid      = "InstanceRoleSsmAttachment"
        Effect   = "Allow"
        Action   = ["iam:AttachRolePolicy", "iam:DetachRolePolicy"]
        Resource = ["arn:aws:iam::${local.account_id}:role/marketlake-instance"]
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
        Resource = ["arn:aws:iam::${local.account_id}:instance-profile/marketlake-instance"]
      },
      {
        # No iam:DeleteUser and no iam:DeleteUserPolicy. A deleted policy stops the
        # nightly upload.
        Sid      = "BackupUserWrite"
        Effect   = "Allow"
        Action   = ["iam:PutUserPolicy", "iam:TagUser", "iam:UntagUser"]
        Resource = ["arn:aws:iam::${local.account_id}:user/marketlake-backup"]
      },
      {
        # infra/live creates this user rather than importing it, so it needs
        # iam:CreateUser. No iam:DeleteUser or iam:DeleteUserPolicy, so CI can never
        # remove the token's only writer, and no iam:CreateAccessKey, whose Deny above
        # keeps the key a hand step.
        Sid      = "TokenWriterUserWrite"
        Effect   = "Allow"
        Action   = ["iam:CreateUser", "iam:PutUserPolicy"]
        Resource = ["arn:aws:iam::${local.account_id}:user/marketlake-token-writer"]
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
    ]
  })
}
