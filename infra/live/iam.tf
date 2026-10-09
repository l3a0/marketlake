# The hosted VM's instance role. It gets the same four S3 actions as the laptop's
# marketlake-backup role in command.tf, which are the ones src/lake/bucket.py calls:
# PutObject, GetObject (which also authorises HEAD), ListBucket and GetBucketVersioning.
# They are split across two policies, a read half that is always on and a write half,
# PutObject alone, meant for a primary VM and not a shadow one. #638's cutover turns it on
# just before the VM's role flips to primary, and the way back turns it off before the role
# returns to shadow. Nothing deletes a version or reads an old one. The instance role also
# reads the config parameters.

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

# Run Command reaches only an SSM-managed instance, and the deploy's SSM document in
# deploy.tf runs over it (#676). This must apply before #686's instance first boots,
# because the SSM agent backs off for up to an hour after failing to authenticate. The
# attachment names the role through its resource, so a combined apply orders it after
# CreateRole. Setting managed_policy_arns on the role, or using
# aws_iam_role_policy_attachments_exclusive, would detach it on every apply.
resource "aws_iam_role_policy_attachment" "instance_ssm" {
  role       = aws_iam_role.instance.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# The write half, which #638's cutover turns on just before the VM's role flips to primary.
# On a shadow host, a role with s3:PutObject would be a write credential to the primary's
# bucket that only the code's `role: shadow` refusal declines to use, so the way back turns
# it off. It keeps the address and the policy name it had before the split (#686).
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
# This is not a statement in either S3 half, for two reasons. The write half is off
# while the VM is a shadow, and the VM needs its config then too. And each half is
# tested as exactly its own S3 actions. AmazonSSMManagedInstanceCore, which #695
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

# -- the users this configuration forgets ----------------------------------------------

# The laptop's two users, marketlake-backup and marketlake-token-writer, gave way to
# marketlake-command and its two roles in command.tf (#737). Each block below drops a
# resource from the state and deletes nothing, so marketlake-backup's key keeps working
# through the migration, and the owner deletes both users by hand once the roles are
# proven. A bare removed block also forgets but warns, and destroy = true would plan a
# delete the apply role cannot run. #741 deletes these blocks once this forget has
# applied, because deleting them first plans that delete.

removed {
  from = aws_iam_user.backup

  lifecycle {
    destroy = false
  }
}

removed {
  from = aws_iam_user_policy.backup

  lifecycle {
    destroy = false
  }
}

removed {
  from = aws_iam_user.token_writer

  lifecycle {
    destroy = false
  }
}

removed {
  from = aws_iam_user_policy.token_writer

  lifecycle {
    destroy = false
  }
}
