# What CI applies: the backup bucket, its IAM user, the instance role, and the user that
# writes the Schwab token. The apply role in infra/bootstrap/roles.tf grants writes on
# exactly these. infra/README.md
# carries the runbook, and docs/design.md's "Infrastructure, defined" carries the
# reasoning.

terraform {
  required_version = "~> 1.13"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # The bucket arrives through `tofu init -backend-config=<file>`. The key stays a
  # literal, because the apply role may write this key and its lock file and nothing
  # else, and tests/component/test_infra_config.py compares the two.
  backend "s3" {
    key          = "live/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}

# No default_tags, so adopting an existing resource plans no tag updates, and the apply
# role needs no S3 tagging action.
provider "aws" {
  region = "us-east-1"
}

# The SSM parameter ARNs in iam.tf name the account, and no tracked file may.
data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
}
