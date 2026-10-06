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
}

run "empty_names_fail_validation" {
  command = plan

  variables {
    backup_bucket      = ""
    backup_policy_name = ""
  }

  expect_failures = [var.backup_bucket, var.backup_policy_name]
}

run "user_and_instance_role_get_the_same_four_actions" {
  command = plan

  variables {
    instance_s3_enabled = true
  }

  assert {
    condition = jsondecode(aws_iam_user_policy.backup.policy).Statement == [
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
    error_message = "marketlake-backup's policy is not exactly the four actions src/lake/bucket.py needs."
  }

  assert {
    condition     = jsondecode(aws_iam_role_policy.instance_s3[0].policy).Statement == jsondecode(aws_iam_user_policy.backup.policy).Statement
    error_message = "marketlake-instance's S3 policy differs from marketlake-backup's."
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

  # The apply role may write only the user named marketlake-token-writer.
  assert {
    condition     = aws_iam_user.token_writer.name == "marketlake-token-writer"
    error_message = "The token writer is not named marketlake-token-writer, the user the apply role may create."
  }

  assert {
    condition     = aws_iam_user_policy.token_writer.user == aws_iam_user.token_writer.name
    error_message = "The token's put policy is not on marketlake-token-writer."
  }

  assert {
    condition     = aws_iam_user_policy.token_writer.name == "put-schwab-oauth-token"
    error_message = "The token writer's policy is not named put-schwab-oauth-token."
  }

  assert {
    condition = jsondecode(aws_iam_user_policy.token_writer.policy).Statement == [
      {
        Effect   = "Allow"
        Action   = ["ssm:PutParameter"]
        Resource = "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config/schwab-oauth-token"
      },
    ]
    error_message = "marketlake-token-writer's policy is not exactly PutParameter on the token parameter."
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
