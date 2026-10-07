variable "backup_bucket" {
  # Not sensitive, because OpenTofu refuses a sensitive import id. The validation turns
  # an empty value into a failed plan rather than a plan that rewrites the policies.
  description = "The lake's backup bucket. CI reads it from the BACKUP_BUCKET secret."
  type        = string
  nullable    = false

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.backup_bucket))
    error_message = "backup_bucket must be an S3 bucket name."
  }
}

variable "backup_policy_name" {
  description = "The name of marketlake-backup's inline policy, read from AWS. CI reads it from the BACKUP_POLICY_NAME variable."
  type        = string
  nullable    = false

  validation {
    condition     = can(regex("^[A-Za-z0-9+=,.@_-]{1,128}$", var.backup_policy_name))
    error_message = "backup_policy_name must be an IAM policy name."
  }
}

variable "adopt_existing" {
  description = "Import the existing bucket and its settings. The tests set it to false, because an import crashes tofu test."
  type        = bool
  default     = true
  nullable    = false
}

variable "instance_s3_enabled" {
  # The cutover pull request for #638 flips this default to true, and inverts the test
  # that asserts it is off. CI passes no value for it, so only this default turns the
  # policy on in CI.
  description = "Give marketlake-instance the backup bucket's four S3 actions."
  type        = bool
  default     = false
  nullable    = false
}
