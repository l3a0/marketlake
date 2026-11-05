# The backup bucket, adopted rather than recreated. Each piece carries prevent_destroy,
# and the infrastructure price in docs/design.md's Tradeoffs names which deletions IAM
# still allows.

import {
  for_each = var.adopt_existing ? toset([var.backup_bucket]) : toset([])
  to       = aws_s3_bucket.backup
  id       = each.value
}

resource "aws_s3_bucket" "backup" {
  bucket = var.backup_bucket

  lifecycle {
    prevent_destroy = true
  }
}

import {
  for_each = var.adopt_existing ? toset([var.backup_bucket]) : toset([])
  to       = aws_s3_bucket_versioning.backup
  id       = each.value
}

# Versioning is what keeps an overwritten partition's old copy. The apply role cannot
# change it, so a change here is applied from an admin session.
resource "aws_s3_bucket_versioning" "backup" {
  bucket = aws_s3_bucket.backup.id

  versioning_configuration {
    status = "Enabled"
  }

  lifecycle {
    prevent_destroy = true
  }
}

import {
  for_each = var.adopt_existing ? toset([var.backup_bucket]) : toset([])
  to       = aws_s3_bucket_server_side_encryption_configuration.backup
  id       = each.value
}

resource "aws_s3_bucket_server_side_encryption_configuration" "backup" {
  bucket = aws_s3_bucket.backup.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

import {
  for_each = var.adopt_existing ? toset([var.backup_bucket]) : toset([])
  to       = aws_s3_bucket_public_access_block.backup
  id       = each.value
}

resource "aws_s3_bucket_public_access_block" "backup" {
  bucket = aws_s3_bucket.backup.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true

  lifecycle {
    prevent_destroy = true
  }
}

import {
  for_each = var.adopt_existing ? toset([var.backup_bucket]) : toset([])
  to       = aws_s3_bucket_lifecycle_configuration.backup
  id       = each.value
}

# Noncurrent versions expire after 30 days under the four paths rewritten every night.
# Partitions keep every version, because with no Object Lock an overwritten
# partition's old version is its only good copy. The fifth rule alone expires current
# objects, the live segments, and docs/design.md's live upload section says why. A test
# compares this whole rule list, so a rule that expires or archives anything else
# changes the test in the same diff.
resource "aws_s3_bucket_lifecycle_configuration" "backup" {
  bucket = aws_s3_bucket.backup.id

  rule {
    id     = "manifest"
    status = "Enabled"

    filter {
      prefix = "lake/manifest.jsonl"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  rule {
    id     = "quarantine"
    status = "Enabled"

    filter {
      prefix = "lake/quarantine.jsonl"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  rule {
    id     = "actions"
    status = "Enabled"

    filter {
      prefix = "lake/actions/"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  rule {
    id     = "journal"
    status = "Enabled"

    filter {
      prefix = "lake/journal/"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  # The bucket is versioned, so the expiry only adds a delete marker and the noncurrent
  # expiry removes the bytes. A rule with days also clears the marker once it stands
  # alone, so no second rule is needed, and S3 refuses expired_object_delete_marker
  # beside days. S3 runs this, so no role needs s3:DeleteObject.
  rule {
    id     = "live"
    status = "Enabled"

    filter {
      prefix = "lake-live/"
    }

    expiration {
      days = 7
    }

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}
