# The SSM document that deploys main to the hosted VM (#676). The deploy role in
# infra/bootstrap/roles.tf may send this document and no other, so the most a deploy
# job can ask of the VM is to move forward to a commit already on main. The host decides
# the rest, in deploy/vm-deploy.sh. docs/design.md's "Infrastructure, defined" carries
# the reasoning, and why AWS-RunShellScript is rejected.
#
# SendCommand can still run an older version of a document, and IAM has no condition key
# that stops it. So a later change that tightens this document gives it a new name.
# Both grants name the prefix marketlake-deploy*, so a new name needs no bootstrap apply.

resource "aws_ssm_document" "deploy" {
  name          = "marketlake-deploy"
  document_type = "Command"

  content = jsonencode({
    schemaVersion = "2.2"
    description   = "Deploy one commit on main to the Marketlake VM through deploy/vm-deploy.sh."
    parameters = {
      # SSM checks each pattern at the API and again on the agent, before it
      # substitutes the value into the step.
      sha = {
        type           = "String"
        description    = "The commit to deploy, 40 hex digits."
        allowedPattern = "^[0-9a-f]{40}$"
      }
      notAfter = {
        type           = "String"
        description    = "The epoch second after which the host refuses to start."
        allowedPattern = "^[0-9]{10}$"
      }
      # Added by #987. It loosens the document, so the name stays, and every older
      # version is tighter than this one. A send that omits it gets the default.
      overrideWindow = {
        type          = "String"
        description   = "true skips the host's deploy window refusal, and nothing else."
        allowedValues = ["false", "true"]
        default       = "false"
      }
    }
    mainSteps = [{
      action = "aws:runShellScript"
      name   = "deploy"
      inputs = {
        # The 90-minute margin the host keeps before a refused span, plus ten minutes.
        # The host's caps sum to at most 80 minutes.
        timeoutSeconds = 6000
        runCommand     = [file("${path.module}/deploy-step.sh")]
      }
    }]
  })
}
