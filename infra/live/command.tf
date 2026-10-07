# The laptop's one AWS identity (#737). The user marketlake-command holds the laptop's
# only long-lived key, and that key can do nothing but assume two roles. The role
# marketlake-backup carries the bucket's four S3 actions, and the role
# marketlake-token-writer can only overwrite the Schwab token. Each call then signs with
# a session that lasts an hour, and CloudTrail records it under the role and the
# session name.
#
# Every ARN here is a literal built from local.account_id, never a resource reference.
# The mock provider returns a random string for a reference, so no test could read the
# account id in it.
#
# All six resources carry prevent_destroy. The apply role may create them and never
# delete them, so a pull request that renames one fails at plan rather than halfway
# through an apply.

# -- the user -----------------------------------------------------------------------

# Its access key is made by hand and stays out of code, so no secret reaches state.
resource "aws_iam_user" "command" {
  name = "marketlake-command"

  lifecycle {
    prevent_destroy = true
  }
}

# This policy is what gates each assume, because each role's trust names the account
# rather than this user. A later MFA requirement (#739) is a Deny added here, which CI
# can apply.
resource "aws_iam_user_policy" "command" {
  name = "assume-command-roles"
  user = aws_iam_user.command.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["sts:AssumeRole"]
        Resource = [
          "arn:aws:iam::${local.account_id}:role/marketlake-backup",
          "arn:aws:iam::${local.account_id}:role/marketlake-token-writer",
        ]
      },
    ]
  })

  lifecycle {
    prevent_destroy = true
  }
}

# -- the roles ----------------------------------------------------------------------

# Each role trusts the account root, on the condition that the caller is
# marketlake-command. Two things rule out a trust that names the user's ARN as its
# principal.
#
# 1. Within one account, a trust that names a user grants the assume by itself, whatever
#    the user's own policy says. With the root as principal, the assume also needs the
#    user's policy above to allow it.
# 2. IAM stores a named user as its unique id, so a recreated user would no longer match.
#    A condition on aws:PrincipalArn compares the ARN, which a recreated user keeps.
#
# The apply role is denied iam:UpdateAssumeRolePolicy, so a trust is written once, at
# CreateRole, and a change to it is applied from an admin session.
#
# Each role sets only its name, its trust and its lifecycle. Everything else stays at the
# provider's default, including the one-hour max_session_duration, so the apply role
# never needs iam:UpdateRole or a tagging action.

resource "aws_iam_role" "backup" {
  name = "marketlake-backup"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::${local.account_id}:root" }
      Action    = "sts:AssumeRole"
      Condition = {
        ArnEquals = { "aws:PrincipalArn" = "arn:aws:iam::${local.account_id}:user/marketlake-command" }
      }
    }]
  })

  lifecycle {
    prevent_destroy = true
  }
}

# The four S3 actions src/lake/bucket.py calls, written as their own literal rather than
# shared with the instance role's, because #638's cutover drops s3:PutObject from this
# role alone.
resource "aws_iam_role_policy" "backup" {
  name = "backup-bucket"
  role = aws_iam_role.backup.name

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

resource "aws_iam_role" "token_writer" {
  name = "marketlake-token-writer"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::${local.account_id}:root" }
      Action    = "sts:AssumeRole"
      Condition = {
        ArnEquals = { "aws:PrincipalArn" = "arn:aws:iam::${local.account_id}:user/marketlake-command" }
      }
    }]
  })

  lifecycle {
    prevent_destroy = true
  }
}

# The laptop's weekly re-auth writes the Schwab token to its parameter (#636), and this
# grant names the parameter in us-east-1 alone. No kms: action, because the AWS-managed
# aws/ssm key lets any principal in the account encrypt through SSM.
resource "aws_iam_role_policy" "token_writer" {
  name = "put-schwab-oauth-token"
  role = aws_iam_role.token_writer.name

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
