# The hosted VM, its security group and key pair, and the lake's own EBS volume (#686).
# The instance is built from code rather than imported, and cloud-init takes it from
# nothing to capturing through the shim in user-data.sh.tftpl, with no login.

locals {
  # A literal rather than a variable, because the lake volume carries prevent_destroy,
  # and changing its zone would replace it. The measurement VM runs in this zone, so it
  # is proven to offer t4g.small in this account. Whether it has a default subnet is
  # still the owner's to confirm before the first apply.
  zone = "us-east-1c"
}

# 099720109477 is Canonical's public publisher account, not the owner's. On 2026-10-07
# the name pattern found ubuntu-noble-24.04-arm64-server-20261004, the newest image, in
# the owner's account. An aws_ssm_parameter lookup of Canonical's AMI parameter is ruled out,
# because tests/component/test_infra_config.py refuses that data type.
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-arm64-server-*"]
  }

  filter {
    name   = "architecture"
    values = ["arm64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnet" "default" {
  vpc_id            = data.aws_vpc.default.id
  availability_zone = local.zone
  default_for_az    = true
}

# The zones that offer instance_type, read by the lake volume's precondition.
data "aws_ec2_instance_type_offerings" "instance_type" {
  location_type = "availability-zone"

  filter {
    name   = "instance-type"
    values = [var.instance_type]
  }
}

# OpenTofu drops a new security group's default egress rule, so the egress block is
# written out. Without it the daemon reaches neither the Schwab API, uv nor GitHub. The
# descriptions carry no apostrophe, because the provider refuses one. A changed group
# description replaces the group.
resource "aws_security_group" "vm" {
  name        = "marketlake-vm"
  description = "SSH from the owner address, and all egress"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH from the owner address"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.owner_ssh_cidr]
  }

  egress {
    description = "Everything, to the Schwab API, uv, GitHub and AWS"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_key_pair" "vm" {
  key_name   = "marketlake-vm"
  public_key = var.ssh_public_key
}

# The lake lives here rather than on the root volume, so replacing the instance never
# deletes captured minutes. The apply role's DenyVolumeDelete refuses its deletion too.
# No kms_key_id, because AWS stores it as an ARN and every plan would show a change.
resource "aws_ebs_volume" "lake" {
  availability_zone = local.zone
  type              = "gp3"
  encrypted         = true
  size              = var.lake_volume_gib

  tags = {
    Name = "marketlake-lake"
  }

  lifecycle {
    prevent_destroy = true

    # On the volume rather than the instance, because the plan halts here first.
    # Otherwise a first apply could create the volume, fail to launch the instance, and
    # leave a volume that prevent_destroy keeps in a zone with no instance type.
    precondition {
      condition     = contains(data.aws_ec2_instance_type_offerings.instance_type.locations, local.zone)
      error_message = "The pinned availability zone does not offer instance_type."
    }
  }
}

resource "aws_instance" "vm" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  subnet_id              = data.aws_subnet.default.id
  vpc_security_group_ids = [aws_security_group.vm.id]
  key_name               = aws_key_pair.vm.key_name
  iam_instance_profile   = aws_iam_instance_profile.instance.name

  # Named rather than left to the default subnet's map_public_ip_on_launch. With no
  # NAT, an instance without a public address reaches neither Schwab, uv nor GitHub.
  associate_public_ip_address = true

  # Once #868's stop is switched on, the VM powers itself off after the day's work, and
  # schedule.tf starts it again (#865). AWS's default for an EBS-backed instance is
  # already stop, and naming it means a poweroff can never terminate the instance.
  instance_initiated_shutdown_behavior = "stop"

  # IMDSv2 only (#663). A hop limit of 1 keeps the metadata service's answers, the
  # role's credentials among them, from crossing a further network hop. The tags are
  # served too, because the config render reads marketlake:backup-target from them.
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
    instance_metadata_tags      = "enabled"
  }

  # Named because an account can change the T4g default, and nothing records that.
  credit_specification {
    cpu_credits = "unlimited"
  }

  # Holds the checkout, the venv, uv's cache and the journal. The size is not measured.
  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.root_volume_gib
    encrypted             = true
    delete_on_termination = true
  }

  # The shim carries the owner's name and the lake volume's id, and nothing secret.
  user_data = templatefile("${path.module}/user-data.sh.tftpl", {
    owner          = "ubuntu"
    lake_volume_id = aws_ebs_volume.lake.id
  })

  # The deploy role in infra/bootstrap/roles.tf may send the deploy document only to an
  # instance whose marketlake:host tag is capture (#676), and deploy/send-deploy.sh finds
  # the VM by the same tag. The config render reads marketlake:backup-target through
  # instance metadata, by the owner's decision of 2026-10-07, so the bucket reaches the
  # VM from the variable OpenTofu already holds rather than from a parameter put by hand.
  # A bucket change updates the tag in place.
  #
  # With instance_metadata_tags enabled, EC2 refuses a tag key holding a / or a space,
  # because the metadata service serves each key as a path. Every key here complies, and
  # tests/component/test_infra_config.py fails on one that does not.
  tags = {
    Name                       = "marketlake"
    "marketlake:host"          = "capture"
    "marketlake:backup-target" = "s3://${var.backup_bucket}/lake"
  }

  # The first boot's SSM agent and config render need both grants in place.
  depends_on = [
    aws_iam_role_policy_attachment.instance_ssm,
    aws_iam_role_policy.instance_config_read,
  ]

  # A new AMI would replace the instance, and a changed user_data would stop and start
  # it. A stopped instance reads back with no public address, which the provider would
  # plan as a replacement. #686 item 4 gives the reasoning. disable_api_stop stays
  # unset, because it would make stop_instance_before_detaching fail.
  lifecycle {
    ignore_changes = [ami, user_data, associate_public_ip_address]
  }
}

# A replacement destroys the attachment before the instance, and detaching a mounted
# volume from a running instance can leave the filesystem dirty, so the provider stops
# the instance first. It never starts it again, so a plan that replaces only the
# attachment leaves the instance stopped.
resource "aws_volume_attachment" "lake" {
  device_name                    = "/dev/sdf"
  volume_id                      = aws_ebs_volume.lake.id
  instance_id                    = aws_instance.vm.id
  stop_instance_before_detaching = true
}
