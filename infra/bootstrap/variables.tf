variable "state_bucket" {
  description = "The S3 bucket that holds both configurations' state."
  type        = string
  nullable    = false

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.state_bucket))
    error_message = "state_bucket must be an S3 bucket name."
  }
}

variable "backup_bucket" {
  description = "The lake's backup bucket, whose objects the plan and apply roles may not read."
  type        = string
  nullable    = false

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.backup_bucket))
    error_message = "backup_bucket must be an S3 bucket name."
  }
}

variable "adopt_existing" {
  description = "Import the existing state bucket. The tests set it to false, because an import crashes tofu test."
  type        = bool
  default     = true
  nullable    = false
}

variable "adopt_github_oidc_provider" {
  description = "Import an existing GitHub OIDC provider. Set it to true only when the account already holds one."
  type        = bool
  default     = false
  nullable    = false
}
