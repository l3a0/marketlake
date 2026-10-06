# Plan-mode checks on the live configuration, against literals. Every run plans,
# because test cleanup cannot destroy a prevent_destroy resource and still reports
# success. prevent_destroy itself is checked by tests/component/test_infra_config.py,
# which reads the .tf files.

mock_provider "aws" {}

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
