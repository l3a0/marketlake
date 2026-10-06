# The laptop's backup user and the hosted VM's instance role. Both get the same four
# S3 actions, which are the ones src/lake/bucket.py calls: PutObject, GetObject (which
# also authorises HEAD), ListBucket and GetBucketVersioning. Nothing deletes a version
# or reads an old one.

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
# hour after failing to authenticate. The role names it through the resource, so a
# combined apply orders it after CreateRole. Setting managed_policy_arns on the role, or
# using aws_iam_role_policy_attachments_exclusive, would detach it on every apply.
resource "aws_iam_role_policy_attachment" "instance_ssm" {
  role       = aws_iam_role.instance.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
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
