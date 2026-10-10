# Four roles. Three are assumed through GitHub's OIDC provider: the plan role, the apply
# role, and the deploy role that .github/workflows/deploy.yml uses (#676). EventBridge
# Scheduler assumes the fourth, the scheduler role, to start the VM once #868's stop is
# switched on (#867).
# Every policy is a jsonencode() literal rather than an aws_iam_policy_document, because
# under tofu test's mock provider that data source returns a random string and no test
# could read the document. The statements are written out in full in each role, rather
# than shared through a local, so tests/component/test_infra_config.py can read them
# from the parse.

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

# Trusted only in the `infra` environment, which needs the owner's approval, and only
# on `main`. The ref check puts a branch check in AWS beside the environment's own.
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
          "token.actions.githubusercontent.com:sub" = "${local.github_subject_prefix}:environment:infra"
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
        # infra/live creates the laptop's one user (#737). No iam:DeleteUser or
        # iam:DeleteUserPolicy, so CI can never take away the laptop's only way in, and no
        # iam:CreateAccessKey, whose Deny above keeps the key a hand step.
        Sid      = "CommandUserWrite"
        Effect   = "Allow"
        Action   = ["iam:CreateUser", "iam:PutUserPolicy"]
        Resource = ["arn:aws:iam::${local.account_id}:user/marketlake-command"]
      },
      {
        # The two roles marketlake-command assumes. The provider puts every setting in
        # CreateRole, and each role sets only its name and trust, so no iam:UpdateRole or
        # tagging action is needed. No iam:DeleteRole, so no plan can replace a role and
        # write a new trust through CreateRole. No iam:DeleteRolePolicy, because a deleted
        # backup policy stops the nightly upload. No iam:PassRole, since no service takes
        # either role.
        Sid    = "CommandRolesWrite"
        Effect = "Allow"
        Action = ["iam:CreateRole", "iam:PutRolePolicy"]
        Resource = [
          "arn:aws:iam::${local.account_id}:role/marketlake-backup",
          "arn:aws:iam::${local.account_id}:role/marketlake-token-writer",
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
        # The deploy's SSM document in infra/live/deploy.tf (#676). The provider calls
        # UpdateDocumentDefaultVersion after every update, and without it SendCommand
        # would keep running the old content. The prefix lets a tightened document take
        # a new name with no bootstrap apply. No ssm:ModifyDocumentPermission, which
        # would share the document with another account.
        Sid    = "DeployDocumentWrite"
        Effect = "Allow"
        Action = [
          "ssm:CreateDocument",
          "ssm:UpdateDocument",
          "ssm:UpdateDocumentDefaultVersion",
          "ssm:DeleteDocument",
        ]
        Resource = ["arn:aws:ssm:us-east-1:${local.account_id}:document/marketlake-deploy*"]
      },
      {
        # The start schedules in infra/live/schedule.tf (#867). Listed one by one, since
        # the wildcard ban allows no other wildcard than ec2:*. The name prefix lets a new
        # schedule land with no bootstrap apply, and the default group is the one the
        # scheduler role's trust names.
        Sid    = "StartSchedulesWrite"
        Effect = "Allow"
        Action = [
          "scheduler:CreateSchedule",
          "scheduler:UpdateSchedule",
          "scheduler:DeleteSchedule",
        ]
        Resource = ["arn:aws:scheduler:us-east-1:${local.account_id}:schedule/default/marketlake-*"]
      },
      {
        # A schedule names the scheduler role as its target's role, and Scheduler checks
        # that the caller may pass it. No grant here writes that role or its policy, and
        # the instance role, which an apply may write, passes only to EC2 under
        # PassTheInstanceRole below. So this is the one role an apply can hand to
        # Scheduler, and an approved apply cannot widen what a schedule may call (#865).
        Sid      = "PassTheSchedulerRole"
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = ["arn:aws:iam::${local.account_id}:role/marketlake-scheduler"]
        Condition = {
          StringEquals = { "iam:PassedToService" = "scheduler.amazonaws.com" }
        }
      },
      {
        # The instance role goes only to EC2, through RunInstances or
        # AssociateIamInstanceProfile. InstanceRoleWrite lets an apply recreate that role
        # with any trust and any policy, so without the condition one approved apply could
        # pass it to Scheduler in a marketlake-* schedule and call ec2:DeleteVolume around
        # DenyVolumeDelete, which binds only this role (#867). Appended after the last
        # statement, per infra/README.md's "Reading a statement inserted into a policy".
        Sid      = "PassTheInstanceRole"
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = ["arn:aws:iam::${local.account_id}:role/marketlake-instance"]
        Condition = {
          StringEquals = { "iam:PassedToService" = "ec2.amazonaws.com" }
        }
      },
    ]
  })
}

# -- the deploy role ----------------------------------------------------------------

# Trusted only in the `deploy` environment, which needs the owner's approval, and only
# on `main`, like the apply role. .github/workflows/deploy.yml assumes it to ask the VM
# to deploy one commit through the SSM document marketlake-deploy, and nothing else.
# Its session lasts five hours, longer than the job's 240 minutes.
resource "aws_iam_role" "deploy" {
  name                 = "marketlake-deploy"
  max_session_duration = 18000

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = local.github_oidc_provider_arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          "token.actions.githubusercontent.com:sub" = "${local.github_subject_prefix}:environment:deploy"
          "token.actions.githubusercontent.com:ref" = "refs/heads/main"
        }
      }
    }]
  })

  depends_on = [aws_iam_openid_connect_provider.github]
}

# No ReadOnlyAccess, and no ssm:CancelCommand, which the deploy never needs and which
# would stop only the host's wrapper anyway. The document marketlake-deploy can only ask
# the VM to move forward to a commit already on main. AWS-RunShellScript would let any
# step in the job run any command as root on the VM, so no grant names it.
resource "aws_iam_role_policy" "deploy" {
  name = "deploy-to-vm"
  role = aws_iam_role.deploy.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # DescribeInstances has no resource-level permissions.
        Sid      = "FindTheVm"
        Effect   = "Allow"
        Action   = ["ec2:DescribeInstances"]
        Resource = ["*"]
      },
      {
        # IAM checks a statement's condition against every resource in the request, and
        # the document carries no marketlake:host tag, so the instance and the document
        # sit in separate statements.
        Sid      = "SendToTheCaptureHost"
        Effect   = "Allow"
        Action   = ["ssm:SendCommand"]
        Resource = ["arn:aws:ec2:us-east-1:${local.account_id}:instance/*"]
        Condition = {
          StringEquals = { "ssm:resourceTag/marketlake:host" = "capture" }
        }
      },
      {
        Sid      = "SendTheDeployDocument"
        Effect   = "Allow"
        Action   = ["ssm:SendCommand"]
        Resource = ["arn:aws:ssm:us-east-1:${local.account_id}:document/marketlake-deploy*"]
      },
      {
        # GetCommandInvocation has no resource types, so this reads any Run Command's
        # output in the account. That is accepted while this deploy is the account's
        # only Run Command user.
        Sid      = "ReadTheDeployResult"
        Effect   = "Allow"
        Action   = ["ssm:GetCommandInvocation"]
        Resource = ["*"]
      },
    ]
  })
}

# -- the scheduler role -------------------------------------------------------------

# EventBridge Scheduler assumes this role when a start schedule in infra/live/schedule.tf
# fires, and it may only start the capture host. It lives here rather than in infra/live,
# because there the apply role would need to write its policy as well as pass it, and one
# approved apply could then call any action through Scheduler's universal target (#865).
# The apply role may pass Scheduler this role alone. The instance role it may write is
# passed only to EC2, under PassTheInstanceRole.
# The trust is scoped to the default schedule group, the only scope AWS documents for
# aws:SourceArn here, and the apply role cannot edit a trust once it exists.
resource "aws_iam_role" "scheduler" {
  name = "marketlake-scheduler"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = {
        StringEquals = {
          "aws:SourceAccount" = local.account_id
          "aws:SourceArn"     = "arn:aws:scheduler:us-east-1:${local.account_id}:schedule-group/default"
        }
      }
    }]
  })
}

# StartInstances on a running instance changes nothing, so a schedule that fires while
# the VM is up is harmless. The tag is the one infra/live/vm.tf gives the VM, and the
# deploy role's SendToTheCaptureHost names it too.
resource "aws_iam_role_policy" "scheduler" {
  name = "start-the-capture-host"
  role = aws_iam_role.scheduler.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "StartTheCaptureHost"
        Effect   = "Allow"
        Action   = ["ec2:StartInstances"]
        Resource = ["arn:aws:ec2:us-east-1:${local.account_id}:instance/*"]
        Condition = {
          StringEquals = { "aws:ResourceTag/marketlake:host" = "capture" }
        }
      },
    ]
  })
}
