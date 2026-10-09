# Plan-mode checks on the live configuration, against literals. Every run plans,
# because test cleanup cannot destroy a prevent_destroy resource and still reports
# success. prevent_destroy and ignore_changes are checked by
# tests/component/test_infra_config.py, which reads the .tf files.

mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "000000000000"
    }
  }

  # The pinned zone, so every run passes the lake volume's precondition. A change to
  # the zone in vm.tf fails every run until this moves with it.
  mock_data "aws_ec2_instance_type_offerings" {
    defaults = {
      locations = ["us-east-1c"]
    }
  }

  # A volume id of the real shape, which the shim writes to bootstrap.conf.
  mock_resource "aws_ebs_volume" {
    defaults = {
      id = "vol-0123456789abcdef0"
    }
  }
}

# The address is from TEST-NET-3, a range reserved for documentation, and the key is not
# a key.
variables {
  backup_bucket      = "example-lake-backup"
  backup_policy_name = "example-policy"
  adopt_existing     = false
  owner_ssh_cidr     = "203.0.113.7/32"
  ssh_public_key     = "ssh-ed25519 AAAAexamplenotakey"
}

run "instance_s3_write_half_is_on_by_default" {
  command = plan

  # #638's cutover pull request flipped the default, as the VM becomes the primary. The
  # way back turns it off again and inverts this assert.
  assert {
    condition     = length(aws_iam_role_policy.instance_s3) == 1
    error_message = "marketlake-instance has no S3 write access by default, so the primary VM's nightly upload would be refused."
  }

  # The read half is compared whole, so a write action moved into it fails here even
  # though the two halves together still equal marketlake-backup's policy.
  assert {
    condition = jsondecode(aws_iam_role_policy.instance_s3_read.policy).Statement == [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketVersioning"]
        Resource = "arn:aws:s3:::example-lake-backup"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "arn:aws:s3:::example-lake-backup/*"
      },
    ]
    error_message = "marketlake-instance's read half is not exactly ListBucket and GetBucketVersioning on the bucket and GetObject on its objects."
  }

  assert {
    condition     = aws_iam_role_policy.instance_s3_read.role == aws_iam_role.instance.name
    error_message = "The S3 read half is not on marketlake-instance."
  }

  assert {
    condition     = aws_iam_role_policy.instance_s3_read.name == "backup-bucket-read"
    error_message = "The S3 read half is not named backup-bucket-read."
  }
}

run "instance_s3_write_half_is_absent_when_disabled" {
  command = plan

  variables {
    instance_s3_enabled = false
  }

  # A shadow VM, before the cutover or after the way back, holds no write credential to
  # the primary's bucket.
  assert {
    condition     = length(aws_iam_role_policy.instance_s3) == 0
    error_message = "Turning instance_s3_enabled off leaves marketlake-instance its S3 write half, a write credential on a shadow host."
  }
}

run "instance_s3_write_half_is_put_alone_when_enabled" {
  command = plan

  variables {
    instance_s3_enabled = true
  }

  assert {
    condition     = length(aws_iam_role_policy.instance_s3) == 1
    error_message = "Turning instance_s3_enabled on does not give marketlake-instance its S3 write half."
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.instance_s3[0].policy).Statement == [
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "arn:aws:s3:::example-lake-backup/*"
      },
    ]
    error_message = "marketlake-instance's write half is not exactly PutObject on the bucket's objects."
  }

  assert {
    condition     = aws_iam_role_policy.instance_s3[0].name == "backup-bucket"
    error_message = "The S3 write half is not named backup-bucket."
  }
}

run "bucket_protections_are_on" {
  command = plan

  assert {
    condition     = aws_s3_bucket_versioning.backup.versioning_configuration[0].status == "Enabled"
    error_message = "The backup's versioning is not Enabled, so an overwritten partition loses its only good copy."
  }

  assert {
    condition = [
      for rule in aws_s3_bucket_server_side_encryption_configuration.backup.rule :
      [for d in rule.apply_server_side_encryption_by_default : d.sse_algorithm]
    ] == [["AES256"]]
    error_message = "The backup's default encryption is not exactly AES256."
  }

  assert {
    condition = [
      aws_s3_bucket_public_access_block.backup.block_public_acls,
      aws_s3_bucket_public_access_block.backup.block_public_policy,
      aws_s3_bucket_public_access_block.backup.ignore_public_acls,
      aws_s3_bucket_public_access_block.backup.restrict_public_buckets,
    ] == [true, true, true, true]
    error_message = "One of the backup's four public access blocks is off."
  }
}

run "instance_role_is_trusted_by_ec2_alone" {
  command = plan

  variables {
    instance_s3_enabled = true
  }

  assert {
    condition = jsondecode(aws_iam_role.instance.assume_role_policy) == {
      Version = "2012-10-17"
      Statement = [{
        Effect    = "Allow"
        Principal = { Service = "ec2.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }]
    }
    error_message = "marketlake-instance's trust is not exactly EC2 assuming the role."
  }

  # The apply role may write only the role named marketlake-instance, so a policy or a
  # profile pointed anywhere else fails the first apply.
  assert {
    condition     = aws_iam_role_policy.instance_s3[0].role == aws_iam_role.instance.name
    error_message = "The S3 write half is not on marketlake-instance."
  }

  assert {
    condition     = aws_iam_instance_profile.instance.role == aws_iam_role.instance.name
    error_message = "The instance profile does not carry marketlake-instance."
  }

  # The apply role may attach the policy only to marketlake-instance, so any other role
  # fails the first apply. A literal role name would also pass here, and the pytest
  # checks that the attachment names the role through its resource.
  assert {
    condition     = aws_iam_role_policy_attachment.instance_ssm.role == aws_iam_role.instance.name
    error_message = "The SSM policy is not attached to marketlake-instance."
  }

  # The apply role may attach exactly this ARN, so any other fails the first apply.
  assert {
    condition     = aws_iam_role_policy_attachment.instance_ssm.policy_arn == "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
    error_message = "The attachment is not AWS's AmazonSSMManagedInstanceCore."
  }
}

run "empty_names_fail_validation" {
  command = plan

  variables {
    backup_bucket      = ""
    backup_policy_name = ""
  }

  expect_failures = [var.backup_bucket, var.backup_policy_name]
}

# Each policy is compared with its own literal rather than with the other, because
# #638's cutover drops s3:PutObject from the backup role alone. The instance role's two
# halves have their own runs above.
run "backup_role_gets_the_four_actions" {
  command = plan

  variables {
    instance_s3_enabled = true
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.backup.policy).Statement == [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketVersioning"]
        Resource = "arn:aws:s3:::example-lake-backup"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetObject"]
        Resource = "arn:aws:s3:::example-lake-backup/*"
      },
    ]
    error_message = "The marketlake-backup role's policy is not exactly the four actions src/lake/bucket.py needs."
  }

  # The apply role may write only the roles named marketlake-backup and
  # marketlake-token-writer.
  assert {
    condition     = aws_iam_role_policy.backup.role == aws_iam_role.backup.name && aws_iam_role.backup.name == "marketlake-backup"
    error_message = "The bucket policy is not on the role named marketlake-backup, the role the apply role may create."
  }

  assert {
    condition     = aws_iam_role_policy.backup.name == "backup-bucket"
    error_message = "The marketlake-backup role's policy is not named backup-bucket."
  }
}

# No variable is set, so instance_s3_enabled keeps its default. The config read is
# always on, whatever the write half's setting, and a count on this policy would make the
# unindexed references below fail.
run "instance_reads_exactly_the_config_parameters" {
  command = plan

  assert {
    condition = jsondecode(aws_iam_role_policy.instance_config_read.policy).Statement == [
      {
        Effect   = "Allow"
        Action   = ["ssm:GetParameter", "ssm:GetParameters"]
        Resource = "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config/*"
      },
    ]
    error_message = "marketlake-instance's config read is not exactly GetParameter and GetParameters on /marketlake/config/*."
  }

  assert {
    condition     = aws_iam_role_policy.instance_config_read.role == aws_iam_role.instance.name
    error_message = "The config read policy is not on marketlake-instance."
  }

  assert {
    condition     = aws_iam_role_policy.instance_config_read.name == "config-parameters-read"
    error_message = "The config read policy is not named config-parameters-read."
  }
}

run "token_writer_puts_only_the_token" {
  command = plan

  assert {
    condition     = aws_iam_role.token_writer.name == "marketlake-token-writer"
    error_message = "The token writer is not named marketlake-token-writer, the role the apply role may create."
  }

  assert {
    condition     = aws_iam_role_policy.token_writer.role == aws_iam_role.token_writer.name
    error_message = "The token's put policy is not on marketlake-token-writer."
  }

  assert {
    condition     = aws_iam_role_policy.token_writer.name == "put-schwab-oauth-token"
    error_message = "The token writer's policy is not named put-schwab-oauth-token."
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.token_writer.policy).Statement == [
      {
        Effect   = "Allow"
        Action   = ["ssm:PutParameter"]
        Resource = "arn:aws:ssm:us-east-1:000000000000:parameter/marketlake/config/schwab-oauth-token"
      },
    ]
    error_message = "marketlake-token-writer's policy is not exactly PutParameter on the token parameter."
  }
}

# A wrong trust is the one mistake CI cannot repair after the merge. The apply role is
# denied iam:UpdateAssumeRolePolicy and never granted iam:DeleteRole, and the plan's step
# summary shows attribute names, never the trust's content. So each trust, and the
# user's policy that gates it, is compared whole.
run "command_roles_trust_only_marketlake_command" {
  command = plan

  assert {
    condition     = aws_iam_user.command.name == "marketlake-command"
    error_message = "The laptop's user is not named marketlake-command, the user the apply role may create."
  }

  assert {
    condition     = aws_iam_user_policy.command.user == aws_iam_user.command.name
    error_message = "The assume policy is not on marketlake-command."
  }

  assert {
    condition     = aws_iam_user_policy.command.name == "assume-command-roles"
    error_message = "marketlake-command's policy is not named assume-command-roles."
  }

  assert {
    condition = jsondecode(aws_iam_user_policy.command.policy) == {
      Version = "2012-10-17"
      Statement = [
        {
          Effect = "Allow"
          Action = ["sts:AssumeRole"]
          Resource = [
            "arn:aws:iam::000000000000:role/marketlake-backup",
            "arn:aws:iam::000000000000:role/marketlake-token-writer",
          ]
        },
      ]
    }
    error_message = "marketlake-command's policy is not exactly sts:AssumeRole on the two roles."
  }

  assert {
    condition = jsondecode(aws_iam_role.backup.assume_role_policy) == {
      Version = "2012-10-17"
      Statement = [{
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::000000000000:root" }
        Action    = "sts:AssumeRole"
        Condition = {
          ArnEquals = { "aws:PrincipalArn" = "arn:aws:iam::000000000000:user/marketlake-command" }
        }
      }]
    }
    error_message = "marketlake-backup's trust is not exactly the account root conditioned on marketlake-command's ARN."
  }

  assert {
    condition = jsondecode(aws_iam_role.token_writer.assume_role_policy) == {
      Version = "2012-10-17"
      Statement = [{
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::000000000000:root" }
        Action    = "sts:AssumeRole"
        Condition = {
          ArnEquals = { "aws:PrincipalArn" = "arn:aws:iam::000000000000:user/marketlake-command" }
        }
      }]
    }
    error_message = "marketlake-token-writer's trust is not exactly the account root conditioned on marketlake-command's ARN."
  }
}

run "lifecycle_rules_are_exactly_the_four" {
  command = plan

  # The whole list is compared, so a rule that expires current versions, archives an
  # object or expires a partition's old copy fails here. The Sunday scrub sees none of
  # those three. Three attributes are computed, and the mock provider fills them with
  # random values, so the comparison sets them to null on both sides. Both sides go
  # through jsonencode, because a typed list never equals a literal tuple.
  assert {
    condition = jsonencode([
      for r in aws_s3_bucket_lifecycle_configuration.backup.rule : merge(r, {
        prefix = null
        filter = [
          for f in r.filter : merge(f, { object_size_greater_than = null, object_size_less_than = null })
        ]
      })
      ]) == jsonencode([
      for rule in [
        ["manifest", "lake/manifest.jsonl"],
        ["quarantine", "lake/quarantine.jsonl"],
        ["actions", "lake/actions/"],
        ["journal", "lake/journal/"],
        ] : {
        id                                = rule[0]
        status                            = "Enabled"
        prefix                            = null
        abort_incomplete_multipart_upload = []
        expiration                        = []
        transition                        = []
        noncurrent_version_transition     = []
        noncurrent_version_expiration     = [{ noncurrent_days = 30, newer_noncurrent_versions = null }]
        filter = [{
          prefix                   = rule[1]
          and                      = []
          tag                      = []
          object_size_greater_than = null
          object_size_less_than    = null
        }]
      }
    ])
    error_message = "The backup's lifecycle rules are not exactly the four noncurrent expiries."
  }
}

# Every other run uses one bucket name, so a policy that typed it would pass them all.
run "bucket_arns_follow_the_bucket_variable" {
  command = plan

  variables {
    backup_bucket = "other-lake-backup"
  }

  assert {
    condition = [for s in jsondecode(aws_iam_role_policy.backup.policy).Statement : s.Resource] == [
      "arn:aws:s3:::other-lake-backup",
      "arn:aws:s3:::other-lake-backup/*",
    ]
    error_message = "marketlake-backup's policy does not name the bucket backup_bucket names."
  }
}

# An ARN or a name with a space would match an unanchored pattern, and "ab" is one
# character short of S3's minimum.
run "an_arn_fails_validation" {
  command = plan

  variables {
    backup_bucket      = "arn:aws:s3:::example-state"
    backup_policy_name = "arn:aws:s3:::example-state"
  }

  expect_failures = [var.backup_bucket, var.backup_policy_name]
}

run "a_two_character_bucket_name_fails_validation" {
  command = plan

  variables {
    backup_bucket = "ab"
  }

  expect_failures = [var.backup_bucket]
}

run "a_name_with_a_space_fails_validation" {
  command = plan

  variables {
    backup_bucket      = "has space"
    backup_policy_name = "has space"
  }

  expect_failures = [var.backup_bucket, var.backup_policy_name]
}

# The mock account id is also what a hard-coded ARN would carry, so plan once under a
# second account. An ARN that does not follow the caller's account names nobody's
# parameter or role, and the VM's read, the token's write and both assumes would be
# refused.
run "parameter_arns_follow_the_callers_account" {
  command = plan

  override_data {
    target = data.aws_caller_identity.current
    values = {
      account_id = "111111111111"
    }
  }

  assert {
    condition = [
      jsondecode(aws_iam_role_policy.instance_config_read.policy).Statement[0].Resource,
      jsondecode(aws_iam_role_policy.token_writer.policy).Statement[0].Resource,
      ] == [
      "arn:aws:ssm:us-east-1:111111111111:parameter/marketlake/config/*",
      "arn:aws:ssm:us-east-1:111111111111:parameter/marketlake/config/schwab-oauth-token",
    ]
    error_message = "A parameter ARN does not name the caller's account."
  }

  # The user's policy and each trust name the account twice, so six ids in all, and
  # every one must be the caller's. Both sides go through jsonencode, because a typed
  # list never equals a literal tuple.
  assert {
    condition = jsonencode(regexall("[0-9]{12}", jsonencode([
      aws_iam_user_policy.command.policy,
      aws_iam_role.backup.assume_role_policy,
      aws_iam_role.token_writer.assume_role_policy,
    ]))) == jsonencode(["111111111111", "111111111111", "111111111111", "111111111111", "111111111111", "111111111111"])
    error_message = "marketlake-command's policy or a command role's trust names an account other than the caller's."
  }
}

# The mock offerings list the pinned zone, so the precondition passes, and the volume
# and the subnet lookup both sit in that zone.
run "lake_volume_sits_in_a_zone_that_offers_the_instance_type" {
  command = plan

  assert {
    condition = [
      aws_ebs_volume.lake.availability_zone,
      data.aws_subnet.default.availability_zone,
    ] == ["us-east-1c", "us-east-1c"]
    error_message = "The lake volume and the instance's subnet are not both in us-east-1c."
  }

  assert {
    condition = [
      aws_ebs_volume.lake.type,
      aws_ebs_volume.lake.encrypted,
      aws_ebs_volume.lake.size,
    ] == ["gp3", true, 30]
    error_message = "The lake volume is not an encrypted 30 GiB gp3 volume."
  }

  assert {
    condition = [
      aws_volume_attachment.lake.volume_id,
      aws_volume_attachment.lake.instance_id,
      aws_volume_attachment.lake.device_name,
      aws_volume_attachment.lake.stop_instance_before_detaching,
    ] == [aws_ebs_volume.lake.id, aws_instance.vm.id, "/dev/sdf", true]
    error_message = "The lake volume is not attached at /dev/sdf with the instance stopped before a detach."
  }
}

# A zone that does not offer instance_type stops the plan at the volume, before a first
# apply could create a volume that no instance in that zone can use.
run "a_zone_without_the_instance_type_fails_the_plan" {
  command = plan

  override_data {
    target = data.aws_ec2_instance_type_offerings.instance_type
    values = {
      locations = []
    }
  }

  expect_failures = [aws_ebs_volume.lake]
}

run "security_group_allows_ssh_from_the_owner_and_all_egress" {
  command = plan

  # Differs from the file-level value, so a literal address in vm.tf fails here.
  variables {
    owner_ssh_cidr = "198.51.100.0/24"
  }

  assert {
    condition = [
      for r in aws_security_group.vm.ingress :
      [
        r.from_port, r.to_port, r.protocol, tolist(r.cidr_blocks),
        r.ipv6_cidr_blocks == null, r.prefix_list_ids == null, r.security_groups == null, r.self == null,
      ]
    ] == [[22, 22, "tcp", tolist(["198.51.100.0/24"]), true, true, true, true]]
    error_message = "The ingress is not exactly TCP 22 from owner_ssh_cidr."
  }

  # OpenTofu drops the default egress rule, so without this the daemon is cut off.
  assert {
    condition = [
      for r in aws_security_group.vm.egress :
      [r.from_port, r.to_port, r.protocol, tolist(r.cidr_blocks)]
    ] == [[0, 0, "-1", tolist(["0.0.0.0/0"])]]
    error_message = "The egress is not exactly every protocol to 0.0.0.0/0."
  }

  assert {
    condition     = [aws_security_group.vm.name, aws_security_group.vm.vpc_id] == ["marketlake-vm", data.aws_vpc.default.id]
    error_message = "The security group is not marketlake-vm in the default VPC."
  }
}

run "instance_is_built_as_the_issue_describes" {
  command = plan

  assert {
    condition = [
      aws_instance.vm.metadata_options[0].http_endpoint,
      aws_instance.vm.metadata_options[0].http_tokens,
      aws_instance.vm.metadata_options[0].http_put_response_hop_limit,
    ] == ["enabled", "required", 1]
    error_message = "The instance does not require IMDSv2 tokens with a hop limit of 1."
  }

  # The config render reads marketlake:backup-target from the metadata service, which
  # serves no tag unless this is enabled.
  assert {
    condition     = aws_instance.vm.metadata_options[0].instance_metadata_tags == "enabled"
    error_message = "The instance does not serve its tags through instance metadata, so the config render finds no backup target."
  }

  assert {
    condition     = aws_instance.vm.credit_specification[0].cpu_credits == "unlimited"
    error_message = "The instance's CPU credits are not named unlimited."
  }

  assert {
    condition     = aws_instance.vm.associate_public_ip_address == true
    error_message = "The instance does not ask for a public address, so with no NAT it reaches nothing."
  }

  # The bucket is the file-level variable's, written out here so the test does not
  # rebuild the tag the way vm.tf builds it.
  assert {
    condition = aws_instance.vm.tags == tomap({
      Name                       = "marketlake"
      "marketlake:host"          = "capture"
      "marketlake:backup-target" = "s3://example-lake-backup/lake"
    })
    error_message = "The instance's tags are not exactly Name, marketlake:host and marketlake:backup-target = s3://example-lake-backup/lake."
  }

  assert {
    condition = [
      aws_instance.vm.instance_type,
      aws_instance.vm.iam_instance_profile,
      aws_instance.vm.key_name,
      aws_instance.vm.subnet_id,
      tolist(aws_instance.vm.vpc_security_group_ids),
      ] == [
      "t4g.small",
      aws_iam_instance_profile.instance.name,
      "marketlake-vm",
      data.aws_subnet.default.id,
      tolist([aws_security_group.vm.id]),
    ]
    error_message = "The instance is not a t4g.small with marketlake-instance, the marketlake-vm key, the zone's subnet and its group."
  }

  assert {
    condition = [
      aws_instance.vm.root_block_device[0].volume_type,
      aws_instance.vm.root_block_device[0].volume_size,
      aws_instance.vm.root_block_device[0].encrypted,
      aws_instance.vm.root_block_device[0].delete_on_termination,
    ] == ["gp3", 16, true, true]
    error_message = "The root volume is not an encrypted 16 GiB gp3 volume deleted with the instance."
  }

  assert {
    condition     = aws_key_pair.vm.public_key == "ssh-ed25519 AAAAexamplenotakey"
    error_message = "The key pair does not carry ssh_public_key."
  }
}

# The tag follows backup_bucket, so a bucket change reaches the VM with the apply.
run "backup_target_tag_follows_the_bucket" {
  command = plan

  variables {
    backup_bucket = "another-lake-backup"
  }

  assert {
    condition     = aws_instance.vm.tags["marketlake:backup-target"] == "s3://another-lake-backup/lake"
    error_message = "The marketlake:backup-target tag does not follow backup_bucket."
  }
}

# Anyone who can describe the instance can read user_data, so the shim carries the
# owner's name and the volume id, as bootstrap.conf's only two lines, and neither SSH
# input.
run "shim_carries_only_the_owner_and_the_volume_id" {
  command = plan

  assert {
    condition     = strcontains(aws_instance.vm.user_data, "<<'CONF'\nOWNER=ubuntu\nLAKE_VOLUME_ID=vol-0123456789abcdef0\nCONF\n")
    error_message = "The shim does not write exactly OWNER=ubuntu and the lake volume's id to bootstrap.conf."
  }

  assert {
    condition = !anytrue([
      for value in [split("/", var.owner_ssh_cidr)[0], var.ssh_public_key, "AAAAexamplenotakey"] :
      strcontains(aws_instance.vm.user_data, value)
    ])
    error_message = "The shim carries the owner's address or SSH key."
  }
}

# The validations refuse a malformed input with a message that names no value. The
# provider refuses some of these too, but its error prints the address.
run "a_bare_address_and_an_empty_key_fail_validation" {
  command = plan

  variables {
    owner_ssh_cidr = "203.0.113.7"
    ssh_public_key = ""
  }

  expect_failures = [var.owner_ssh_cidr, var.ssh_public_key]
}

# The ingress rule's cidr_blocks takes IPv4 only.
run "an_ipv6_cidr_fails_validation" {
  command = plan

  variables {
    owner_ssh_cidr = "2001:db8::/64"
  }

  expect_failures = [var.owner_ssh_cidr]
}

run "a_cidr_with_host_bits_fails_validation" {
  command = plan

  variables {
    owner_ssh_cidr = "203.0.113.7/24"
  }

  expect_failures = [var.owner_ssh_cidr]
}

# A home address is a /32. A prefix shorter than /16 opens SSH to far more than one
# owner, and 0.0.0.0/0 opens it to everyone.
run "an_open_cidr_fails_validation" {
  command = plan

  variables {
    owner_ssh_cidr = "0.0.0.0/0"
  }

  expect_failures = [var.owner_ssh_cidr]
}

run "a_slash_eight_fails_validation" {
  command = plan

  variables {
    owner_ssh_cidr = "10.0.0.0/8"
  }

  expect_failures = [var.owner_ssh_cidr]
}

run "a_slash_fifteen_fails_validation" {
  command = plan

  variables {
    owner_ssh_cidr = "10.0.0.0/15"
  }

  expect_failures = [var.owner_ssh_cidr]
}

run "a_slash_sixteen_passes_validation" {
  command = plan

  variables {
    owner_ssh_cidr = "10.0.0.0/16"
  }

  assert {
    condition     = [for r in aws_security_group.vm.ingress : tolist(r.cidr_blocks)] == [tolist(["10.0.0.0/16"])]
    error_message = "A /16 does not reach the ingress rule."
  }
}

run "a_slash_twenty_four_passes_validation" {
  command = plan

  variables {
    owner_ssh_cidr = "203.0.113.0/24"
  }

  assert {
    condition     = [for r in aws_security_group.vm.ingress : tolist(r.cidr_blocks)] == [tolist(["203.0.113.0/24"])]
    error_message = "A /24 does not reach the ingress rule."
  }
}

run "a_slash_thirty_two_passes_validation" {
  command = plan

  variables {
    owner_ssh_cidr = "203.0.113.7/32"
  }

  assert {
    condition     = [for r in aws_security_group.vm.ingress : tolist(r.cidr_blocks)] == [tolist(["203.0.113.7/32"])]
    error_message = "A /32 does not reach the ingress rule."
  }
}
