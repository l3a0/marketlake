# What CI needs before it can run: the state bucket, GitHub's OIDC provider, and the
# plan, apply and deploy roles. The owner applies this from the laptop, because the CI
# it creates cannot apply it. infra/README.md carries the runbook, and docs/design.md's
# "Infrastructure, defined" carries the reasoning.

terraform {
  required_version = "~> 1.13"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # The bucket arrives through `tofu init -backend-config=<file>`, so no tracked file
  # names it. Both keys stay literals, because the apply role in roles.tf may write
  # only infra/live's key, and tests/component/test_infra_config.py checks the pair.
  backend "s3" {
    key          = "bootstrap/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}

# No default_tags, so adopting an existing resource plans no tag updates.
provider "aws" {
  region = "us-east-1"
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id

  # One provider per issuer in an account, so this ARN is fixed once the account is.
  github_oidc_provider_arn = "arn:aws:iam::${local.account_id}:oidc-provider/token.actions.githubusercontent.com"
}

# -- the state bucket ---------------------------------------------------------------

# The bucket is created by one `aws s3api create-bucket` and then adopted here. A new
# bucket comes encrypted and with public access blocked, but not versioned.
import {
  for_each = var.adopt_existing ? toset([var.state_bucket]) : toset([])
  to       = aws_s3_bucket.state
  id       = each.value
}

resource "aws_s3_bucket" "state" {
  bucket = var.state_bucket

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id

  versioning_configuration {
    status = "Enabled"
  }

  lifecycle {
    prevent_destroy = true
  }
}

# -- GitHub's OIDC provider ---------------------------------------------------------

# Importing a provider that does not exist fails the plan, so this import has its own
# switch, off unless the owner's check found one.
import {
  for_each = var.adopt_github_oidc_provider ? toset([local.github_oidc_provider_arn]) : toset([])
  to       = aws_iam_openid_connect_provider.github
  id       = each.value
}

resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}
