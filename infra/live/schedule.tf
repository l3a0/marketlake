# The two schedules that start the VM before capture needs it (#867). Once #868's stop is
# switched on, the VM stops itself after the day's work, and nothing else would start it
# again. #865 carries the reasoning for each setting, and
# tests/component/test_infra_config.py checks each start against the roster in
# src/lake/deploy_window.py.
#
# Each schedule passes the role marketlake-scheduler, which infra/bootstrap/roles.tf
# declares and which may only call ec2:StartInstances on the capture host. Its ARN is a
# literal built from local.account_id, as command.tf builds its ARNs, because a
# data "aws_iam_role" lookup fails every mocked tofu test run.
#
# Both schedules sit in the default group, the only one the role's trust accepts, and
# take its name prefix marketlake-, the only one the apply role may write. Neither sets
# kms_key_arn, so Scheduler encrypts with its own key and needs no KMS grant.
#
# Each writes its state out, and the live test checks that it is "ENABLED". To stop a
# schedule from firing, set its state to "DISABLED" in a pull request that also edits
# that test, so the reviewer sees that the disable is on purpose.

# 07:30 every weekday, holidays included, because every weekday check still expects its
# ping on a holiday. That is an hour before the 08:30 self-check, and ten minutes before
# the 07:40 morning check that #868 adds to page when the VM is not up.
resource "aws_scheduler_schedule" "start_weekday" {
  name                         = "marketlake-start-weekday"
  schedule_expression          = "cron(30 7 ? * MON-FRI *)"
  schedule_expression_timezone = "America/New_York"
  state                        = "ENABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = "arn:aws:scheduler:::aws-sdk:ec2:startInstances"
    role_arn = "arn:aws:iam::${local.account_id}:role/marketlake-scheduler"

    # Scheduler does not check this input until the schedule fires, so the live test
    # compares it with an exact string.
    input = jsonencode({ InstanceIds = [aws_instance.vm.id] })

    # AWS's own limits, written out. A start that first succeeds at 10:00 still saves
    # the rest of the session. A retry that lands at night is harmless, since once
    # #868's stop is switched on the VM stops itself again after its hour. Scheduler
    # retries only the errors it treats as retryable, so a refused call is not retried.
    retry_policy {
      maximum_event_age_in_seconds = 86400
      maximum_retry_attempts       = 185
    }
  }
}

# 19:30 every Sunday, half an hour before the 20:00 Sunday job.
resource "aws_scheduler_schedule" "start_sunday" {
  name                         = "marketlake-start-sunday"
  schedule_expression          = "cron(30 19 ? * SUN *)"
  schedule_expression_timezone = "America/New_York"
  state                        = "ENABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = "arn:aws:scheduler:::aws-sdk:ec2:startInstances"
    role_arn = "arn:aws:iam::${local.account_id}:role/marketlake-scheduler"
    input    = jsonencode({ InstanceIds = [aws_instance.vm.id] })

    retry_policy {
      maximum_event_age_in_seconds = 86400
      maximum_retry_attempts       = 185
    }
  }
}
