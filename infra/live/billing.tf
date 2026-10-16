# The cost and usage export and the bucket it delivers to (#912, part of #911). AWS Data
# Exports writes the account's cost and usage report, in its CUR 2.0 form, to the bucket
# as Parquet, and the VM's nightly sweep reads it through the grant in iam.tf and the
# marketlake:cost-export tag in vm.tf. #911 carries the reasoning, and its "The contract
# between the halves" fixes the export's name, its prefix and the tag.
#
# The owner keeps every month of the raw export indefinitely (owner, 2026-10-16), so the
# bucket and the export carry prevent_destroy. Every argument of the export except its
# query forces a replacement, so prevent_destroy also turns a change that would delete
# and recreate the export into a failed plan.

# Only the bucket and its policy are declared. A new bucket comes encrypted and with
# public access blocked, as the state bucket in infra/bootstrap/main.tf relies on, and
# with ACLs disabled. Versioning stays off by owner decision on 2026-10-16, because each
# refresh rewrites the month's file, and with versioning on the bucket would keep every
# rewrite. No import block, because nothing creates this bucket by hand.
resource "aws_s3_bucket" "billing" {
  bucket = var.billing_bucket

  lifecycle {
    prevent_destroy = true
  }
}

# AWS's own policy for an export's bucket, from the Data Exports guide's
# dataexports-s3-bucket.html: the service may put objects, and only for an export in
# this account. It grants no read and no delete.
resource "aws_s3_bucket_policy" "billing" {
  bucket = aws_s3_bucket.billing.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "EnableAWSDataExportsToWriteToS3"
        Effect    = "Allow"
        Principal = { Service = ["bcm-data-exports.amazonaws.com"] }
        Action    = ["s3:PutObject"]
        Resource  = "arn:aws:s3:::${var.billing_bucket}/*"
        Condition = {
          ArnLike = {
            "aws:SourceArn" = "arn:aws:bcm-data-exports:us-east-1:${local.account_id}:export/*"
          }
          StringEquals = {
            "aws:SourceAccount" = local.account_id
          }
        }
      },
    ]
  })
}

# The data lands at cur/marketlake-cost/data/BILLING_PERIOD=YYYY-MM/, and each refresh
# overwrites the month's files in place. SYNCHRONOUS is the only cadence the API takes,
# and it refreshes up to three times a day.
#
# table_configurations sets the five keys the provider's acceptance tests read back from
# GetExport. A map that differs from what GetExport returns would plan a replacement on
# every run, which prevent_destroy turns into a failed plan. The first plan after the
# apply is what shows the map matches.
#
# The query is the one argument that changes in place. Its first nine columns are the
# ones the sweep's figures read (#913). The rest are kept for questions the figures do not
# answer, since the raw export is kept indefinitely.
#
# The provider records the export in state only after it reports healthy. A create whose
# wait fails leaves the export in AWS and outside state, and infra/README.md's
# "Recover an export the apply left outside state" gives the recovery.
resource "aws_bcmdataexports_export" "cost" {
  export {
    name = "marketlake-cost"

    data_query {
      query_statement = "SELECT ${join(", ", [
        "line_item_usage_start_date",
        "line_item_product_code",
        "line_item_usage_type",
        "line_item_resource_id",
        "line_item_line_item_type",
        "line_item_unblended_cost",
        "line_item_usage_amount",
        "line_item_currency_code",
        "line_item_usage_account_id",
        "identity_line_item_id",
        "identity_time_interval",
        "bill_billing_period_start_date",
        "line_item_usage_end_date",
        "line_item_operation",
        "line_item_line_item_description",
        "line_item_unblended_rate",
        "pricing_unit",
        "product_servicecode",
        "product_region_code",
        "product",
        "resource_tags",
      ])} FROM COST_AND_USAGE_REPORT"

      table_configurations = {
        COST_AND_USAGE_REPORT = {
          TIME_GRANULARITY                      = "DAILY"
          INCLUDE_RESOURCES                     = "TRUE"
          INCLUDE_SPLIT_COST_ALLOCATION_DATA    = "FALSE"
          INCLUDE_MANUAL_DISCOUNT_COMPATIBILITY = "FALSE"
          BILLING_VIEW_ARN                      = "arn:aws:billing::${local.account_id}:billingview/primary"
        }
      }
    }

    destination_configurations {
      s3_destination {
        s3_bucket = aws_s3_bucket.billing.bucket
        s3_prefix = "cur"
        s3_region = "us-east-1"

        s3_output_configurations {
          output_type = "CUSTOM"
          format      = "PARQUET"
          compression = "PARQUET"
          overwrite   = "OVERWRITE_REPORT"
        }
      }
    }

    refresh_cadence {
      frequency = "SYNCHRONOUS"
    }
  }

  # Creating the export checks that the destination's policy lets the service write.
  depends_on = [aws_s3_bucket_policy.billing]

  lifecycle {
    prevent_destroy = true
  }
}

locals {
  # s3://<billing bucket>/cur/marketlake-cost, the value of the VM's marketlake:cost-export
  # tag, built from the export's own destination and name so the two cannot disagree.
  cost_export_destination = aws_bcmdataexports_export.cost.export[0].destination_configurations[0].s3_destination[0]
  cost_export_target      = "s3://${local.cost_export_destination.s3_bucket}/${local.cost_export_destination.s3_prefix}/${aws_bcmdataexports_export.cost.export[0].name}"
}
