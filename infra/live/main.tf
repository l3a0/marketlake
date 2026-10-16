# What CI applies: the backup bucket, the instance role, the laptop's identity, the
# user marketlake-command with the two roles it assumes, marketlake-backup to read the
# bucket and marketlake-token-writer for the Schwab token, the hosted VM with its lake
# volume, the SSM document that deploys main to the VM, the two schedules that start
# the VM once #868's stop is switched on, and the cost and usage export with its billing
# bucket (#912). The apply role in infra/bootstrap/roles.tf grants writes on exactly
# these. infra/README.md carries the runbook, and docs/design.md's "Infrastructure,
# defined" carries the reasoning.

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

# The ARNs in iam.tf, command.tf, schedule.tf and billing.tf name the account, which no
# tracked file may name.
data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
}
