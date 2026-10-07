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
  description = "Import the existing bucket, user and policy. The tests set it to false, because an import crashes tofu test."
  type        = bool
  default     = true
  nullable    = false
}

variable "instance_s3_enabled" {
  # The cutover pull request for #638 flips this default to true, and inverts the test
  # that asserts it is off. CI passes no value for it, so only this default turns the
  # write half on in CI.
  description = "Give marketlake-instance s3:PutObject on the backup bucket, the write half of its S3 access. The read half is always on."
  type        = bool
  default     = false
  nullable    = false
}

variable "owner_ssh_cidr" {
  # Sensitive, so the owner's address stays out of plan output. It still sits in state
  # as plain text, and the plan role can read it through ec2:DescribeSecurityGroups.
  # The provider refuses an IPv6 CIDR, or one with host bits set, but its error prints
  # the address, and a plan's errors reach CI's public log. The first validation refuses
  # both before the provider sees them, with a message that names no value. The second refuses a prefix shorter
  # than /16, so 0.0.0.0/0 cannot open SSH to the whole internet. A home address is a
  # /32, and /16 leaves room for a provider's range.
  description = "The owner's address that may SSH to the VM, as an IPv4 CIDR such as a /32. CI reads it from the OWNER_SSH_CIDR repository secret."
  type        = string
  sensitive   = true
  nullable    = false

  validation {
    condition     = can(cidrnetmask(var.owner_ssh_cidr)) && try(cidrhost(var.owner_ssh_cidr, 0) == split("/", var.owner_ssh_cidr)[0], false)
    error_message = "owner_ssh_cidr must be an IPv4 CIDR with no host bits set, such as a /32."
  }

  validation {
    condition     = try(tonumber(split("/", var.owner_ssh_cidr)[1]) >= 16, false)
    error_message = "owner_ssh_cidr must have a prefix of /16 or longer, such as a /32."
  }
}

variable "ssh_public_key" {
  # EC2 imports RSA and ED25519 keys, whose OpenSSH form starts with ssh-.
  description = "The OpenSSH public key for the VM's key pair. CI reads it from the SSH_PUBLIC_KEY repository variable."
  type        = string
  nullable    = false

  validation {
    condition     = startswith(var.ssh_public_key, "ssh-")
    error_message = "ssh_public_key must be an OpenSSH public key starting with ssh-."
  }
}

variable "instance_type" {
  # Unmeasured until #633's sizing verdict lands. A change stops and starts the
  # instance.
  description = "The VM's instance type."
  type        = string
  default     = "t4g.small"
  nullable    = false
}

variable "root_volume_gib" {
  # Not measured. The root holds the checkout, the venv, uv's cache and the journal.
  description = "The VM's root volume size, in GiB."
  type        = number
  default     = 16
  nullable    = false
}

variable "lake_volume_gib" {
  # From a 7.9 GB lake growing 0.58 GB a night, 30 GiB holds roughly 40 more sessions,
  # derived rather than measured. A larger size modifies the volume in place, and the
  # bootstrap's resize2fs grows the filesystem to match.
  description = "The lake volume's size, in GiB."
  type        = number
  default     = 30
  nullable    = false
}
