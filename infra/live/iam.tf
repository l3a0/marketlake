# The laptop's backup user and the hosted VM's instance role. The user holds the four
# S3 actions src/lake/bucket.py calls: PutObject, GetObject (which also authorises
# HEAD), ListBucket and GetBucketVersioning. The role holds the same four split across
# two policies, a read half that is always on and a write half, PutObject alone, that
# stays off until #638's cutover. Nothing deletes a version or reads an old one. The
# instance role also reads the config parameters, and a third principal, the laptop's
# token writer, can only overwrite the Schwab token (#699).

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

# Run Command reaches only an SSM-managed instance, and #676 deploys over it. This must
# apply before #686's instance first boots, because the SSM agent backs off for up to an
# hour after failing to authenticate. The attachment names the role through its
# resource, so a combined apply orders it after CreateRole. Setting managed_policy_arns
# on the role, or using aws_iam_role_policy_attachments_exclusive, would detach it on
# every apply.
resource "aws_iam_role_policy_attachment" "instance_ssm" {
  role       = aws_iam_role.instance.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# The write half, off until #638's cutover. On a shadow host, a role with s3:PutObject
# would be a write credential to the primary's bucket that only the code's
# `role: shadow` refusal declines to use. It keeps the address and the policy name it
# had before the split (#686).
resource "aws_iam_role_policy" "instance_s3" {
  count = var.instance_s3_enabled ? 1 : 0

  name = "backup-bucket"
  role = aws_iam_role.instance.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "arn:aws:s3:::${var.backup_bucket}/*"
      },
    ]
  })
}

# The read half, always on, so #640's restore runs on the VM through the instance
# profile with no stored key. Its name differs from the write half's, because
# PutRolePolicy on a shared name would make one overwrite the other.
resource "aws_iam_role_policy" "instance_s3_read" {
  name = "backup-bucket-read"
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
        Action   = ["s3:GetObject"]
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
# This is not a statement in either S3 half, because the two halves' statements are
# tested together against marketlake-backup's. AmazonSSMManagedInstanceCore, which #695
# attaches to the same role, allows both actions on every parameter, so it sets the
# role's real read scope. This grant keeps the read from depending on that managed
# policy. No kms: action is needed, because the parameters use the AWS-managed aws/ssm
# key. The backup target is not a parameter. The render reads it from the instance's
# marketlake:backup-target tag through instance metadata, which needs no IAM grant.
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
