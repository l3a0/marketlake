# The laptop's backup user and the hosted VM's instance role. Both get the same four
# S3 actions, which are the ones src/lake/bucket.py calls: PutObject, GetObject (which
# also authorises HEAD), ListBucket and GetBucketVersioning. Nothing deletes a version
# or reads an old one. The instance role also reads the config parameters, and a third
# principal, the laptop's token writer, can only overwrite the Schwab token (#699).

# -- the backup user ----------------------------------------------------------------

import {
  for_each = var.adopt_existing ? toset(["marketlake-backup"]) : toset([])
  to       = aws_iam_user.backup
  id       = each.value
}

# Its access key is made by hand and stays out of code, so no secret reaches state.
resource "aws_iam_user" "backup" {
  name = "marketlake-backup"

  lifecycle {
    prevent_destroy = true
  }
}

import {
  for_each = var.adopt_existing ? toset(["marketlake-backup:${var.backup_policy_name}"]) : toset([])
  to       = aws_iam_user_policy.backup
  id       = each.value
}

resource "aws_iam_user_policy" "backup" {
  name = var.backup_policy_name
  user = aws_iam_user.backup.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketVersioning"]
        Resource = "arn:aws:s3:::${var.backup_bucket}"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetObject"]
        Resource = "arn:aws:s3:::${var.backup_bucket}/*"
      },
    ]
  })

  lifecycle {
    prevent_destroy = true
  }
}

# -- the instance role --------------------------------------------------------------

# #663 makes the bucket client use this role, and #686 attaches its profile to the VM.
# Its trust names only EC2. The apply role cannot rewrite a trust policy in place, so
# a change here is applied from an admin session.
resource "aws_iam_role" "instance" {
  name = "marketlake-instance"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Off until #638's cutover. On a shadow host, a role with s3:PutObject would be a write
# credential to the primary's bucket that only the code's `role: shadow` refusal
# declines to use.
resource "aws_iam_role_policy" "instance_s3" {
  count = var.instance_s3_enabled ? 1 : 0

  name = "backup-bucket"
  role = aws_iam_role.instance.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketVersioning"]
        Resource = "arn:aws:s3:::${var.backup_bucket}"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetObject"]
        Resource = "arn:aws:s3:::${var.backup_bucket}/*"
      },
    ]
  })
}

resource "aws_iam_instance_profile" "instance" {
  name = "marketlake-instance"
  role = aws_iam_role.instance.name
}

# The VM's config.yaml secrets and the Schwab token, as SecureString parameters (#699).
# This is not a statement in instance_s3, for two reasons. That policy is off until the
# cutover, and the VM needs its config on the shadow day. And its statements are tested
# equal to marketlake-backup's. Until #695 attaches AmazonSSMManagedInstanceCore, this
# is the role's only SSM read. After that, the managed policy, which allows both actions
# on every parameter, sets the role's real read scope. No kms: action is needed, because
# the parameters use the AWS-managed aws/ssm key.
resource "aws_iam_role_policy" "instance_config_read" {
  name = "config-parameters-read"
  role = aws_iam_role.instance.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["ssm:GetParameter", "ssm:GetParameters"]
        Resource = "arn:aws:ssm:us-east-1:${local.account_id}:parameter/marketlake/config/*"
      },
    ]
  })
}

# -- the token writer ---------------------------------------------------------------

# The laptop's weekly re-auth writes the Schwab token to its parameter (#636). It has
# its own user, by the owner's decision of 2026-10-06, because marketlake-backup's key
# loses its write grants at the cutover. A leaked key from this user can only overwrite
# the token. Its access key is made by hand, as the backup user's is, so no secret
# reaches state. The apply role may create it but never delete it, so it is created
# here rather than imported, and never by hand.
resource "aws_iam_user" "token_writer" {
  name = "marketlake-token-writer"

  lifecycle {
    prevent_destroy = true
  }
}

# No kms: action, because the AWS-managed aws/ssm key lets any principal in the account
# encrypt through SSM.
resource "aws_iam_user_policy" "token_writer" {
  name = "put-schwab-oauth-token"
  user = aws_iam_user.token_writer.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["ssm:PutParameter"]
        Resource = "arn:aws:ssm:us-east-1:${local.account_id}:parameter/marketlake/config/schwab-oauth-token"
      },
    ]
  })

  lifecycle {
    prevent_destroy = true
  }
}
