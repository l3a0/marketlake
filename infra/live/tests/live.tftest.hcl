# Plan-mode checks on the live configuration, against literals. Every run plans,
# because test cleanup cannot destroy a prevent_destroy resource and still reports
# success. prevent_destroy itself is checked by tests/component/test_infra_config.py,
# which reads the .tf files.

mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "000000000000"
    }
  }
}

variables {
  backup_bucket      = "example-lake-backup"
  backup_policy_name = "example-policy"
  adopt_existing     = false
}

run "instance_s3_policy_is_off_by_default" {
  command = plan

  # #638's cutover pull request flips the default and inverts this assert.
  assert {
    condition     = length(aws_iam_role_policy.instance_s3) == 0
    error_message = "marketlake-instance has S3 access by default. It would be a write credential on a shadow host."
  }
}

run "instance_s3_policy_is_on_when_enabled" {
  command = plan

  variables {
    instance_s3_enabled = true
  }

  assert {
    condition     = length(aws_iam_role_policy.instance_s3) == 1
    error_message = "Turning instance_s3_enabled on does not give marketlake-instance its S3 policy."
  }
}

run "bucket_protections_are_on" {
  command = plan

  assert {
    condition     = aws_s3_bucket_versioning.backup.versioning_configuration[0].status == "Enabled"
    error_message = "The backup's versioning is not Enabled, so an overwritten partition loses its only good copy."
  }

  assert {
    condition = [
      for rule in aws_s3_bucket_server_side_encryption_configuration.backup.rule :
      [for d in rule.apply_server_side_encryption_by_default : d.sse_algorithm]
    ] == [["AES256"]]
    error_message = "The backup's default encryption is not exactly AES256."
  }

  assert {
    condition = [
      aws_s3_bucket_public_access_block.backup.block_public_acls,
      aws_s3_bucket_public_access_block.backup.block_public_policy,
      aws_s3_bucket_public_access_block.backup.ignore_public_acls,
      aws_s3_bucket_public_access_block.backup.restrict_public_buckets,
    ] == [true, true, true, true]
    error_message = "One of the backup's four public access blocks is off."
  }
}

run "instance_role_is_trusted_by_ec2_alone" {
  command = plan

  variables {
    instance_s3_enabled = true
  }

  assert {
    condition = jsondecode(aws_iam_role.instance.assume_role_policy) == {
      Version = "2012-10-17"
      Statement = [{
        Effect    = "Allow"
        Principal = { Service = "ec2.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }]
    }
    error_message = "marketlake-instance's trust is not exactly EC2 assuming the role."
  }

  # The apply role may write only the role named marketlake-instance, so a policy or a
  # profile pointed anywhere else fails the first apply.
  assert {
    condition     = aws_iam_role_policy.instance_s3[0].role == aws_iam_role.instance.name
    error_message = "The S3 policy is not on marketlake-instance."
  }

  assert {
    condition     = aws_iam_instance_profile.instance.role == aws_iam_role.instance.name
    error_message = "The instance profile does not carry marketlake-instance."
  }

  # The apply role may attach the policy only to marketlake-instance, so any other role
  # fails the first apply. A literal role name would also pass here, and the pytest
  # checks that the attachment names the role through its resource.
  assert {
    condition     = aws_iam_role_policy_attachment.instance_ssm.role == aws_iam_role.instance.name
    error_message = "The SSM policy is not attached to marketlake-instance."
  }

  # The apply role may attach exactly this ARN, so any other fails the first apply.
  assert {
    condition     = aws_iam_role_policy_attachment.instance_ssm.policy_arn == "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
    error_message = "The attachment is not AWS's AmazonSSMManagedInstanceCore."
  }
}

run "empty_names_fail_validation" {
  command = plan

  variables {
    backup_bucket      = ""
    backup_policy_name = ""
  }

  expect_failures = [var.backup_bucket, var.backup_policy_name]
}

# Each policy is compared with its own literal rather than with the other, because
# #638's cutover drops s3:PutObject from the backup role alone.
run "backup_and_instance_roles_get_the_same_four_actions" {
  command = plan

  variables {
    instance_s3_enabled = true
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.backup.policy).Statement == [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketVersioning"]
        Resource = "arn:aws:s3:::example-lake-backup"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetObject"]
        Resource = "arn:aws:s3:::example-lake-backup/*"
      },
    ]
    error_message = "The marketlake-backup role's policy is not exactly the four actions src/lake/bucket.py needs."
  }

  # The apply role may write only the roles named marketlake-backup and
  # marketlake-token-writer.
  assert {
    condition     = aws_iam_role_policy.backup.role == aws_iam_role.backup.name && aws_iam_role.backup.name == "marketlake-backup"
    error_message = "The bucket policy is not on the role named marketlake-backup, the role the apply role may create."
  }

  assert {
    condition     = aws_iam_role_policy.backup.name == "backup-bucket"
    error_message = "The marketlake-backup role's policy is not named backup-bucket."
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.instance_s3[0].policy).Statement == [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketVersioning"]
        Resource = "arn:aws:s3:::example-lake-backup"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetObject"]
        Resource = "arn:aws:s3:::example-lake-backup/*"
      },
    ]
    error_message = "marketlake-instance's S3 policy is not exactly the four actions src/lake/bucket.py needs."
  }
}

# No variable is set, so instance_s3_enabled keeps its default of false. The VM reads
# its config on the shadow day, before the cutover turns the S3 policy on, and a count
# on this policy would make the unindexed references below fail.
run "instance_reads_exactly_the_config_parameters" {
  command = plan

  assert {
    condition = jsondecode(aws_iam_role_policy.instance_config_read.policy).Statement == [
      {
        Effect   = "Allow"
        Action   = ["ssm:GetParameter", "ssm:GetParameters"]
        Resource = "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config/*"
      },
    ]
    error_message = "marketlake-instance's config read is not exactly GetParameter and GetParameters on /marketlake/config/*."
  }

  assert {
    condition     = aws_iam_role_policy.instance_config_read.role == aws_iam_role.instance.name
    error_message = "The config read policy is not on marketlake-instance."
  }

  assert {
    condition     = aws_iam_role_policy.instance_config_read.name == "config-parameters-read"
    error_message = "The config read policy is not named config-parameters-read."
  }
}

run "token_writer_puts_only_the_token" {
  command = plan

  assert {
    condition     = aws_iam_role.token_writer.name == "marketlake-token-writer"
    error_message = "The token writer is not named marketlake-token-writer, the role the apply role may create."
  }

  assert {
    condition     = aws_iam_role_policy.token_writer.role == aws_iam_role.token_writer.name
    error_message = "The token's put policy is not on marketlake-token-writer."
  }

  assert {
    condition     = aws_iam_role_policy.token_writer.name == "put-schwab-oauth-token"
    error_message = "The token writer's policy is not named put-schwab-oauth-token."
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.token_writer.policy).Statement == [
      {
        Effect   = "Allow"
        Action   = ["ssm:PutParameter"]
        Resource = "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config/schwab-oauth-token"
      },
    ]
    error_message = "marketlake-token-writer's policy is not exactly PutParameter on the token parameter."
  }
}

# A wrong trust is the one mistake CI cannot repair after the merge. The apply role is
# denied iam:UpdateAssumeRolePolicy and never granted iam:DeleteRole, and the plan's step
# summary shows attribute names, never the trust's content. So each trust, and the
# user's policy that gates it, is compared whole.
run "command_roles_trust_only_marketlake_command" {
  command = plan

  assert {
    condition     = aws_iam_user.command.name == "marketlake-command"
    error_message = "The laptop's user is not named marketlake-command, the user the apply role may create."
  }

  assert {
    condition     = aws_iam_user_policy.command.user == aws_iam_user.command.name
    error_message = "The assume policy is not on marketlake-command."
  }

  assert {
    condition     = aws_iam_user_policy.command.name == "assume-command-roles"
    error_message = "marketlake-command's policy is not named assume-command-roles."
  }

  assert {
    condition = jsondecode(aws_iam_user_policy.command.policy) == {
      Version = "2012-10-17"
      Statement = [
        {
          Effect = "Allow"
          Action = ["sts:AssumeRole"]
          Resource = [
            "arn:aws:iam::000000000000:role/marketlake-backup",
            "arn:aws:iam::000000000000:role/marketlake-token-writer",
          ]
        },
      ]
    }
    error_message = "marketlake-command's policy is not exactly sts:AssumeRole on the two roles."
  }

  assert {
    condition = jsondecode(aws_iam_role.backup.assume_role_policy) == {
      Version = "2012-10-17"
      Statement = [{
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::000000000000:root" }
        Action    = "sts:AssumeRole"
        Condition = {
          ArnEquals = { "aws:PrincipalArn" = "arn:aws:iam::000000000000:user/marketlake-command" }
        }
      }]
    }
    error_message = "marketlake-backup's trust is not exactly the account root conditioned on marketlake-command's ARN."
  }

  assert {
    condition = jsondecode(aws_iam_role.token_writer.assume_role_policy) == {
      Version = "2012-10-17"
      Statement = [{
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::000000000000:root" }
        Action    = "sts:AssumeRole"
        Condition = {
          ArnEquals = { "aws:PrincipalArn" = "arn:aws:iam::000000000000:user/marketlake-command" }
        }
      }]
    }
    error_message = "marketlake-token-writer's trust is not exactly the account root conditioned on marketlake-command's ARN."
  }
}

run "lifecycle_rules_are_exactly_the_four" {
  command = plan

  # The whole list is compared, so a rule that expires current versions, archives an
  # object or expires a partition's old copy fails here. The Sunday scrub sees none of
  # those three. Three attributes are computed, and the mock provider fills them with
  # random values, so the comparison sets them to null on both sides. Both sides go
  # through jsonencode, because a typed list never equals a literal tuple.
  assert {
    condition = jsonencode([
      for r in aws_s3_bucket_lifecycle_configuration.backup.rule : merge(r, {
        prefix = null
        filter = [
          for f in r.filter : merge(f, { object_size_greater_than = null, object_size_less_than = null })
        ]
      })
      ]) == jsonencode([
      for rule in [
        ["manifest", "lake/manifest.jsonl"],
        ["quarantine", "lake/quarantine.jsonl"],
        ["actions", "lake/actions/"],
        ["journal", "lake/journal/"],
        ] : {
        id                                = rule[0]
        status                            = "Enabled"
        prefix                            = null
        abort_incomplete_multipart_upload = []
        expiration                        = []
        transition                        = []
        noncurrent_version_transition     = []
        noncurrent_version_expiration     = [{ noncurrent_days = 30, newer_noncurrent_versions = null }]
        filter = [{
          prefix                   = rule[1]
          and                      = []
          tag                      = []
          object_size_greater_than = null
          object_size_less_than    = null
        }]
      }
    ])
    error_message = "The backup's lifecycle rules are not exactly the four noncurrent expiries."
  }
}

# Every other run uses one bucket name, so a policy that typed it would pass them all.
run "bucket_arns_follow_the_bucket_variable" {
  command = plan

  variables {
    backup_bucket = "other-lake-backup"
  }

  assert {
    condition = [for s in jsondecode(aws_iam_role_policy.backup.policy).Statement : s.Resource] == [
      "arn:aws:s3:::other-lake-backup",
      "arn:aws:s3:::other-lake-backup/*",
    ]
    error_message = "marketlake-backup's policy does not name the bucket backup_bucket names."
  }
}

# An ARN or a name with a space would match an unanchored pattern, and "ab" is one
# character short of S3's minimum.
run "an_arn_fails_validation" {
  command = plan

  variables {
    backup_bucket      = "arn:aws:s3:::example-state"
    backup_policy_name = "arn:aws:s3:::example-state"
  }

  expect_failures = [var.backup_bucket, var.backup_policy_name]
}

run "a_two_character_bucket_name_fails_validation" {
  command = plan

  variables {
    backup_bucket = "ab"
  }

  expect_failures = [var.backup_bucket]
}

run "a_name_with_a_space_fails_validation" {
  command = plan

  variables {
    backup_bucket      = "has space"
    backup_policy_name = "has space"
  }

  expect_failures = [var.backup_bucket, var.backup_policy_name]
}

# The mock account id is also what a hard-coded ARN would carry, so plan once under a
# second account. An ARN that does not follow the caller's account names nobody's
# parameter or role, and the VM's read, the token's write and both assumes would be
# refused.
run "parameter_arns_follow_the_callers_account" {
  command = plan

  override_data {
    target = data.aws_caller_identity.current
    values = {
      account_id = "111111111111"
    }
  }

  assert {
    condition = [
      jsondecode(aws_iam_role_policy.instance_config_read.policy).Statement[0].Resource,
      jsondecode(aws_iam_role_policy.token_writer.policy).Statement[0].Resource,
      ] == [
      "arn:aws:ssm:us-east-1:111111111111:parameter/marketlake/config/*",
      "arn:aws:ssm:us-east-1:111111111111:parameter/marketlake/config/schwab-oauth-token",
    ]
    error_message = "A parameter ARN does not name the caller's account."
  }

  # The user's policy and each trust name the account twice, so six ids in all, and
  # every one must be the caller's. Both sides go through jsonencode, because a typed
  # list never equals a literal tuple.
  assert {
    condition = jsonencode(regexall("[0-9]{12}", jsonencode([
      aws_iam_user_policy.command.policy,
      aws_iam_role.backup.assume_role_policy,
      aws_iam_role.token_writer.assume_role_policy,
    ]))) == jsonencode(["111111111111", "111111111111", "111111111111", "111111111111", "111111111111", "111111111111"])
    error_message = "marketlake-command's policy or a command role's trust names an account other than the caller's."
  }
}
