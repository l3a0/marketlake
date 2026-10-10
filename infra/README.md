# Infrastructure runbook

The hosted deployment gets rebuilt at the cutover from the laptop to a hosted VM, after a
VM dies, at a resize, and in a disaster restore. With its AWS resources in code, each
rebuild is one reviewed change instead of a walk through the console, and this runbook is
the part that code cannot do for itself. The design's
[Deployment section](../docs/design.md#deployment-laptop-now-dedicated-later) carries the
reasoning, under "Infrastructure, defined", and
[#664](https://github.com/l3a0/marketlake/issues/664) carries the plan.

The code is two OpenTofu configurations. A plan lists the changes OpenTofu would make, and
an apply makes them. OpenTofu keeps a record of which real resources each configuration
manages, called its state, and both states sit in one S3 bucket under separate keys.

1. `infra/bootstrap/` holds what CI needs before it can run: the state bucket, GitHub's
   OIDC provider, and three roles that GitHub Actions assumes without a stored AWS key.
   The OIDC provider lets a workflow trade a short-lived token that GitHub signs for
   temporary AWS credentials. The plan role reads, and pull requests may assume it. The
   apply role writes, and only the `infra` environment on `main` may assume it. The
   deploy role can only ask the VM to deploy a commit, and only the `deploy` environment
   on `main` may assume it. CI cannot apply the configuration that creates them, so the
   owner applies this one from the laptop.
2. `infra/live/` holds the backup bucket, the instance role `marketlake-instance` with its
   read of the bucket and the config parameters, the laptop's one IAM user,
   `marketlake-command` ([#737](https://github.com/l3a0/marketlake/issues/737)), and the
   hosted VM with its security group, key pair and lake volume, and the SSM document
   `marketlake-deploy` that [Deploy from CI](#deploy-from-ci) sends to the VM. That user
   can only assume two roles: `marketlake-backup`, which reaches the bucket, and
   `marketlake-token-writer`, which writes the Schwab token's parameter.
   `.github/workflows/infra.yml` plans it on each pull request from a branch here, and
   applies it after a merge to `main` once the owner approves the run. Either starts on
   its own only when the pull request or merge changes the workflow or a file under
   `infra/` other than Markdown, since no configuration reads Markdown. A manual run can
   also replace the VM, as [The hosted VM](#the-hosted-vm) says.

The laptop also applies any change the apply role may not make, such as the backup
bucket's versioning or a role's trust policy. A trust policy says who may assume the
role, that is, take on its permissions.

This repository is public, so every value below appears as a placeholder. Four values
stay out of every tracked file, every issue and every comment.

1. The account id, which step 2 prints and no command below needs.
2. `<state-bucket>`, the state bucket's name.
3. `<backup-bucket>`, the backup bucket's name.
4. The owner's home address, which may SSH to the VM. The commands below read it from
   the network and never print it.

The rest are read when they are needed.

- `<github-user-id>` is the owner's numeric GitHub id, which step 8 reads.
- `<backup-key-id>` is the id of the old `marketlake-backup` user's access key, and
  `<policy-name>` is the name of that user's inline policy. Both are read in
  [Retire the old users](#retire-the-old-users).
- `<switch-time>` is when the laptop's `config.yaml` moved to `assume_role`, in UTC as
  `2026-10-07T21:00:00Z`. The CloudTrail lookup reads every event since then.
- `<branch>` and `<n>` are the pull request's branch and number.
- `<run-id>`, `<lock-id>`, `<branch-head>` and `<merge-commit>` are ids that an earlier
  command prints, named where each one appears.
- `<instance-id>` and `<vm-address>` are the VM's instance id and public address, which
  [Find the VM's address](#find-the-vms-address) prints.

Every placeholder on a command line sits inside double quotes, because a bare `<x>` left
unreplaced is a redirection in zsh.

## The first run, in order

This section records the run for
[PR #698](https://github.com/l3a0/marketlake/pull/698), merged 2026-10-06, which added
`infra.yml`. A rerun with `infra.yml` already on `main`, such as a rebuild into a fresh
account, differs in four steps.

1. Step 3 finds no `marketlake-backup` user, so it skips the user lookups.
2. Step 7 applies the bootstrap from a worktree at `origin/main` rather than at a pull
   request's branch.
3. Step 10 drops out, since no pull request needs a plan.
4. Step 11 replaces the merge with a manual run of `infra.yml` on `main`, which is
   `gh workflow run infra.yml --repo l3a0/marketlake --ref main`. Its first apply in a
   fresh account also creates `aws_iam_role_policy_attachment.instance_ssm`, which
   [#695](https://github.com/l3a0/marketlake/issues/695) added after the recorded run, and
   `marketlake-command` with its two roles, which
   [#737](https://github.com/l3a0/marketlake/issues/737) added. After that apply, create
   the user's access key and read the roles' ARNs by hand, as
   [Create the token writer, in order](#create-the-token-writer-in-order) says.

### 1. Install the tools

```bash
brew install awscli opentofu
```

`aws login` needs AWS CLI 2.32.0 or later, so check the version.

```bash
aws --version
```

OpenTofu must be 1.13 or a later 1.x release, which both configurations require. CI pins
its own copy in `infra.yml`. AWS CloudShell, the browser terminal in the AWS console, does
not work for any of this, because its 1 GB home directory cannot hold the AWS provider,
the plugin OpenTofu downloads to talk to AWS.

### 2. Open an admin session under a named profile

```bash
aws login --profile marketlake-admin
```

The first `aws login` prompts for a region. Answer `us-east-1`.

Never run a plain `aws login`. It writes the `default` profile, and then every process on
the laptop that looks for AWS credentials without naming a profile, agent sessions
included, acts as account admin. Check that the session works.

```bash
aws sts get-caller-identity --profile marketlake-admin
```

It prints the account id. Never paste that id into an issue or a comment. Without the
profile, no credentials should be found, so the same command without it fails.

```bash
aws sts get-caller-identity
```

A command run through Claude Code's `!` prefix may start a fresh shell, so an `export`
does not last from one command to the next. That is why every `aws` command below passes
`--profile marketlake-admin` and every `tofu` command starts with
`AWS_PROFILE=marketlake-admin`.

### 3. Read the values recorded nowhere

Nothing tracked names the backup bucket, or says whether the account already holds a
GitHub OIDC provider, so read both from AWS.

List the buckets, to find `<backup-bucket>`.

```bash
aws s3api list-buckets --profile marketlake-admin
```

The next two commands read the old `marketlake-backup` user, which the recorded run
imported. [#737](https://github.com/l3a0/marketlake/issues/737) replaced that user with a
role of the same name and stopped importing it. The owner deleted the user on 2026-10-09,
so there is no user left to read, and a rerun skips both.

List the IAM users, to confirm `marketlake-backup` exists.

```bash
aws iam list-users --profile marketlake-admin
```

List the user's inline policies. An import adopts a resource that already exists into
the state, rather than creating it. The recorded run's import of the user's policy needed
the one name this printed, so there was no safe guess.

```bash
aws iam list-user-policies --user-name marketlake-backup --profile marketlake-admin
```

Read the backup bucket's lifecycle rules. The output may open in a pager, a full-screen
viewer like `less`. Press `q` to leave it.

```bash
aws s3api get-bucket-lifecycle-configuration --bucket "<backup-bucket>" --profile marketlake-admin
```

The code in `infra/live/bucket.tf` expects four rules, with these ids.

1. `manifest`
2. `quarantine`
3. `actions`
4. `journal`

Each one has a `Filter.Prefix` under `lake/` and expires noncurrent versions after 30
days. A different id or shape shows up in the first plan as an in-place update. On
2026-10-06 the four rules matched the code.

List the account's OIDC providers.

```bash
aws iam list-open-id-connect-providers --profile marketlake-admin
```

An account holds one provider per issuer. When the list shows
`token.actions.githubusercontent.com`, set `adopt_github_oidc_provider = true` in step 4's
`bootstrap.tfvars`, so the bootstrap adopts that provider rather than failing to create a
second. On 2026-10-06 no provider existed, so the value stayed `false`.

### 4. Write the four input files

The laptop keeps each configuration's inputs in `~/.config/marketlake/infra/`, outside
every checkout. A new git worktree, a second checkout of the repository, starts without
the files git ignores, so inputs kept inside a checkout would be missing from it.

```bash
mkdir -p ~/.config/marketlake/infra
```

`bootstrap.tfbackend` names the state bucket for `infra/bootstrap/`. OpenTofu calls the
place it keeps state the backend. Choose a new name for `<state-bucket>`. S3 bucket names
are unique across all of AWS, so a common name may be taken.

```bash
printf 'bucket = "%s"\n' "<state-bucket>" > ~/.config/marketlake/infra/bootstrap.tfbackend
```

`bootstrap.tfvars` holds the three values the bootstrap needs.

```bash
cat > ~/.config/marketlake/infra/bootstrap.tfvars <<'EOF'
state_bucket               = "<state-bucket>"
backup_bucket              = "<backup-bucket>"
adopt_github_oidc_provider = false
EOF
```

`live.tfbackend` names the same bucket for `infra/live/`.

```bash
printf 'bucket = "%s"\n' "<state-bucket>" > ~/.config/marketlake/infra/live.tfbackend
```

`live.tfvars` holds the backup bucket's name. The recorded run also wrote
`backup_policy_name`, which [#741](https://github.com/l3a0/marketlake/issues/741) removed.

```bash
cat > ~/.config/marketlake/infra/live.tfvars <<'EOF'
backup_bucket = "<backup-bucket>"
EOF
```

Since [#686](https://github.com/l3a0/marketlake/issues/686) added the VM, the file needs
two more lines, which [Add the VM's two lines to `live.tfvars`](#add-the-vms-two-lines-to-livetfvars)
writes.

### 5. Create the state bucket

```bash
aws s3api create-bucket --bucket "<state-bucket>" --region us-east-1 --profile marketlake-admin
```

In `us-east-1` the command takes no `--create-bucket-configuration`. A new bucket comes
encrypted and with public access blocked, but not versioned. The bootstrap adopts it and
turns versioning on.

### 6. Check the repository's OIDC subject format

Each role's trust policy matches the `sub` claim, a field GitHub writes into each token.
Read the format this repository uses before applying those policies, because a mismatch
needs a code change and checking first costs nothing.

```bash
gh api repos/l3a0/marketlake/actions/oidc/customization/sub
```

The `sub` conditions in `infra/bootstrap/roles.tf` must start with the `sub_claim_prefix`
it returns. On 2026-10-06 it returned `use_immutable_subject: true`, which makes GitHub
write the owner's and the repository's numeric ids into each subject. A trust policy
written as `repo:l3a0/marketlake:*` would then match no token at all. A prefix that
differs from the code's needs a code change before step 7, and a role whose trust matches
nothing fails the plan job in step 10 at its credentials step.

### 7. Apply the bootstrap from a separate worktree

Apply the bootstrap from the pull request's branch at its final head, in a worktree of
its own. Never switch the main checkout to the branch, because the main checkout is the
code the live daemon runs. The first two commands run in the main checkout, where a new
shell starts.

```bash
git fetch origin
```

```bash
git worktree add --detach "$HOME/marketlake-infra-<n>" "origin/<branch>"
```

Every later command names the worktree, with `git -C` or `tofu -chdir`, rather than
changing into it. A command run through Claude Code's `!` prefix may start a fresh shell,
and a `cd` to a directory outside the project may not last into the next command.

Check that the worktree sits at the pull request's final head. The two commands should
print the same full commit id.

```bash
git -C "$HOME/marketlake-infra-<n>" rev-parse HEAD
```

```bash
gh pr view "<n>" --repo l3a0/marketlake --json headRefOid --jq .headRefOid
```

Initialize the configuration against the state bucket.

```bash
AWS_PROFILE=marketlake-admin tofu -chdir="$HOME/marketlake-infra-<n>/infra/bootstrap" init -backend-config="$HOME/.config/marketlake/infra/bootstrap.tfbackend"
```

Plan it, and read the plan before applying anything.

```bash
AWS_PROFILE=marketlake-admin tofu -chdir="$HOME/marketlake-infra-<n>/infra/bootstrap" plan -var-file="$HOME/.config/marketlake/infra/bootstrap.tfvars"
```

On a first run today the plan imports the state bucket and creates ten resources.

1. The state bucket's versioning.
2. The GitHub OIDC provider.
3. The plan role.
4. The plan role's attachment of AWS's `ReadOnlyAccess`.
5. The plan role's inline policy.
6. The apply role.
7. The apply role's attachment of `ReadOnlyAccess`.
8. The apply role's inline policy.
9. The deploy role, `marketlake-deploy`, which
   [#676](https://github.com/l3a0/marketlake/issues/676) added after the recorded run.
10. The deploy role's inline policy.

When step 3 found an existing provider, the plan imports the provider instead of creating
it, so it shows two imports and nine creates. The plan destroys nothing. A later run,
against state that already exists, plans only what the pull request changes. Treat a plan
that destroys anything as coming from a stale checkout until shown otherwise.

```bash
AWS_PROFILE=marketlake-admin tofu -chdir="$HOME/marketlake-infra-<n>/infra/bootstrap" apply -var-file="$HOME/.config/marketlake/infra/bootstrap.tfvars"
```

The apply plans again and prints its own summary line. Type `yes` only if that line
matches the plan just read. On 2026-10-06 the first apply reported 1 imported, 8 added,
0 changed and 0 destroyed.

### 8. Create the `infra` environment

Only the owner runs this step, and only the owner approves a deployment. An agent session
never approves, rejects or cancels a deployment, and never edits an environment or its
protection rules. That rule is the "Deployment approvals" section of
[`CLAUDE.md`](../CLAUDE.md#deployment-approvals-owner-directive-2026-10-06).

The environment must exist before the first apply job starts, because GitHub creates a
missing environment with no protection rules. On the first run, the merge that added
`infra.yml` started its apply job at once. On a rerun, the manual run that replaces the
merge in step 11 starts it. Read the owner's numeric id first. It is `<github-user-id>`
below.

```bash
gh api user --jq .id
```

Create the environment with the owner as the required reviewer and a custom branch
policy.

```bash
gh api -X PUT repos/l3a0/marketlake/environments/infra --input - <<'EOF'
{"prevent_self_review": false,
 "can_admins_bypass": false,
 "reviewers": [{"type": "User", "id": <github-user-id>}],
 "deployment_branch_policy": {"protected_branches": false, "custom_branch_policies": true}}
EOF
```

Each setting has a reason.

1. `prevent_self_review` is off, because agent sessions merge as the owner, and the owner
   could not approve a run that their own merge started.
2. Setting `can_admins_bypass` to `false` is optional. GitHub defaults it to `true`, which
   lets a repository admin deploy past the approval, and every agent session acts with
   the owner's admin token. GitHub's published REST description of this `PUT` call does
   not list the field, although the `GET` returns it, so the read-back below is what shows
   whether it took effect. The owner's 2026-10-06 command left the line out, so the
   environment kept `true`. Leave the line out to keep the default.
3. The reviewer is the owner alone.
4. The custom branch policy lets the next command limit deployments to `main`.

```bash
gh api -X POST repos/l3a0/marketlake/environments/infra/deployment-branch-policies -f name=main -f type=branch
```

Read the protection back before anything merges. The first command must list
`required_reviewers` and `branch_policy`.

```bash
gh api repos/l3a0/marketlake/environments/infra --jq '[.protection_rules[].type]'
```

The second must print `["main"]`.

```bash
gh api repos/l3a0/marketlake/environments/infra/deployment-branch-policies --jq '[.branch_policies[].name]'
```

The third prints `false` when the `can_admins_bypass` line took effect, and `true` when it
was left out or ignored.

```bash
gh api repos/l3a0/marketlake/environments/infra --jq .can_admins_bypass
```

A rebuild also creates the `deploy` environment, which arrived after the recorded run.
[The `deploy` environment and its secret](#the-deploy-environment-and-its-secret) gives
its commands.

### 9. Set the secrets and the variables

Nothing tracked names the account, the buckets, the owner's address or the VM's key, so
the workflow reads them from six settings.

1. The secret `AWS_APPLY_ROLE_ARN`, the apply role's ARN, the identifier AWS gives each
   resource. It sits on the `infra` environment, so the apply job can read it only after
   the approval.
2. The secret `AWS_PLAN_ROLE_ARN`, the plan role's ARN, on the repository.
3. The secret `TF_STATE_BUCKET`, the state bucket's name, on the repository.
4. The secret `BACKUP_BUCKET`, the backup bucket's name, on the repository.
5. The secret `OWNER_SSH_CIDR`, the owner's address as a `/32`, the one address that may
   SSH to the VM, on the repository.
6. The variable `SSH_PUBLIC_KEY`, the public half of the VM's SSH key, on the
   repository.

The last two arrived with the VM in [#686](https://github.com/l3a0/marketlake/issues/686),
after the recorded run. Every setting but the first sits on the repository, because the
pull request's `plan` job runs in no environment and reads only repository values.

Each command below takes its value from AWS or from a placeholder, so no value lands in a
tracked file.

```bash
aws iam get-role --role-name marketlake-apply --query Role.Arn --output text --profile marketlake-admin | gh secret set AWS_APPLY_ROLE_ARN --env infra --repo l3a0/marketlake
```

```bash
aws iam get-role --role-name marketlake-plan --query Role.Arn --output text --profile marketlake-admin | gh secret set AWS_PLAN_ROLE_ARN --repo l3a0/marketlake
```

```bash
printf '%s' "<state-bucket>" | gh secret set TF_STATE_BUCKET --repo l3a0/marketlake
```

```bash
printf '%s' "<backup-bucket>" | gh secret set BACKUP_BUCKET --repo l3a0/marketlake
```

The owner's address comes from AWS's address echo service, which answers with the
address the request came from. The command pipes it straight into the secret, so it
never appears on screen or in shell history. The plan refuses an IPv6 address, a CIDR
with host bits set, and a prefix shorter than `/16`, each with a message that names no
value. The `/16` floor keeps a pasted `0.0.0.0/0` from opening SSH to the whole internet,
and still leaves room for a provider's range.

```bash
printf '%s/32' "$(curl -fsS https://checkip.amazonaws.com)" | gh secret set OWNER_SSH_CIDR --repo l3a0/marketlake
```

The VM's key pair takes the public half of the laptop's existing key,
`~/.ssh/marketlake_vm.pub`, which also reaches the measurement VM. The owner set the
variable from it on 2026-10-07. If the key does not exist, as in a rebuild on a new
laptop, generate it first with this command.

```bash
ssh-keygen -t ed25519 -f ~/.ssh/marketlake_vm -C marketlake_vm
```

```bash
gh variable set SSH_PUBLIC_KEY --body "$(cat ~/.ssh/marketlake_vm.pub)" --repo l3a0/marketlake
```

Then check the names against what the workflow reads. This lists every secret and
variable `infra.yml` names.

```bash
grep -oE '(secrets|vars)\.[A-Z_]+' "$HOME/marketlake-infra-<n>/.github/workflows/infra.yml" | sort -u
```

The repository's secrets should include the four repository-scoped names,
`AWS_PLAN_ROLE_ARN`, `TF_STATE_BUCKET`, `BACKUP_BUCKET` and `OWNER_SSH_CIDR`.

```bash
gh secret list --repo l3a0/marketlake
```

The environment's secrets should include `AWS_APPLY_ROLE_ARN`.

```bash
gh secret list --env infra --repo l3a0/marketlake
```

The repository's variables should include `SSH_PUBLIC_KEY`.

```bash
gh variable list --repo l3a0/marketlake
```

A rebuild also sets `AWS_DEPLOY_ROLE_ARN` on the `deploy` environment, as
[The `deploy` environment and its secret](#the-deploy-environment-and-its-secret) says.

### 10. Re-run the pull request's plan

The `tofu plan (live)` job fails until steps 7 and 9 are done, because it refuses an
empty secret and cannot assume a role that does not exist yet. Find the pull request's
latest run of `infra.yml`.

```bash
gh run list --repo l3a0/marketlake --branch "<branch>" --workflow infra.yml --limit 1
```

Re-run its failed jobs, with the id the list printed in place of `<run-id>`.

```bash
gh run rerun "<run-id>" --failed --repo l3a0/marketlake
```

The plan appears only in the run page's job summary, never in the log. The repository is
public, so the job sends OpenTofu's output to `/dev/null` and writes only addresses,
actions and attribute names to the summary. On the first run the summary should show
these actions, as it did on 2026-10-06.

| Address | Action |
| --- | --- |
| `aws_iam_instance_profile.instance` | create |
| `aws_iam_role.instance` | create |
| `aws_iam_user.backup` | no-op (import) |
| `aws_iam_user_policy.backup` | no-op (import) |
| `aws_s3_bucket.backup` | no-op (import) |
| `aws_s3_bucket_lifecycle_configuration.backup` | no-op (import) |
| `aws_s3_bucket_public_access_block.backup` | no-op (import) |
| `aws_s3_bucket_server_side_encryption_configuration.backup` | no-op (import) |
| `aws_s3_bucket_versioning.backup` | no-op (import) |

The bucket, its four settings, the user and its policy import with no change. The
instance role and its profile are the only creates, and nothing is destroyed or
replaced. Before the merge, adopt any in-place update the summary shows into code, or
list it on the pull request's issue.

### 11. Merge, approve, and confirm nothing is left to change

Merge the pull request. Then the owner approves the `tofu apply (live)` run in the
Actions tab, outside 09:25 to 16:15 ET, so an apply never runs while the market is open
and the daemon is capturing.

A run that waits for approval waits in the `infra` environment. To approve it, open the
run in the Actions tab, click **Review deployments**, tick `infra`, then click **Approve
and deploy**. Only the owner clicks these, as step 8 says.

After the first green apply, two runs confirm that nothing is left to change.

1. A manual run of `infra.yml` on `main` waits for the owner's approval like any other
   apply, and its apply should find nothing to change.
2. A laptop plan of `infra/bootstrap/` from `main`'s merge commit, in the same worktree,
   should report no changes.

Start the manual run.

```bash
gh workflow run infra.yml --repo l3a0/marketlake --ref main
```

Move the worktree to `main`'s head.

```bash
git -C "$HOME/marketlake-infra-<n>" fetch origin
```

```bash
git -C "$HOME/marketlake-infra-<n>" switch --detach origin/main
```

Check it. The two commands should print the same full commit id.

```bash
git -C "$HOME/marketlake-infra-<n>" rev-parse HEAD
```

```bash
gh api repos/l3a0/marketlake/commits/main --jq .sha
```

The `-reconfigure` flag sets the backend up again from the file and never offers to move
state.

```bash
AWS_PROFILE=marketlake-admin tofu -chdir="$HOME/marketlake-infra-<n>/infra/bootstrap" init -reconfigure -backend-config="$HOME/.config/marketlake/infra/bootstrap.tfbackend"
```

```bash
AWS_PROFILE=marketlake-admin tofu -chdir="$HOME/marketlake-infra-<n>/infra/bootstrap" plan -var-file="$HOME/.config/marketlake/infra/bootstrap.tfvars"
```

A change here means a review fix was pushed after the bootstrap apply, or a stale
checkout applied the bootstrap.

Then confirm again that the environment is protected.

```bash
gh api repos/l3a0/marketlake/environments/infra --jq '[.protection_rules[].type]'
```

It must still list `required_reviewers` and `branch_policy`. An unprotected environment
named `infra` runs the same apply, and its run is just as green.

A later merge that changes `infra.yml`, or anything under `infra/` other than Markdown,
while a run waits starts a newer run. After its approval, the older run checks `main`
again, finds the newer commit, and skips itself as stale. The newer run applies
everything once the owner approves it. So approving waiting runs out of order never
applies an older commit last. A merge that changes only Markdown under `infra/` starts
no run, and the older run reads it as fresh and applies.

### 12. End the session

```bash
aws logout --profile marketlake-admin
```

An admin session lasts up to 12 hours, and anything on the laptop that sets the profile
can apply without approval until it ends. Then remove the worktree, from the main
checkout.

```bash
git worktree remove "$HOME/marketlake-infra-<n>"
```

## Apply `infra/live/` from the laptop

Some changes to `infra/live/` need more than the apply role may do, such as the backup
bucket's versioning or a role's trust policy. Merge such a change first, then apply it
from the laptop under the admin profile. Three things set this apply apart from CI's.

1. It bypasses the approval gate, so only the owner's own reading of the plan stands
   before the apply.
2. It shares the CI apply's state lock, the file beside the state that stops two runs
   writing it at once. That file is `live/terraform.tfstate.tflock`, so whichever of the
   two starts second fails on the lock.
3. It must come from `main`'s head. A checkout that predates a merged change reads the
   same state and plans that change away.

With no approval gate, two rules that an approval would enforce fall to the owner. Once
the VM exists, start it if it is stopped, as in
[Start a stopped instance](#start-a-stopped-instance), before running any plan. A started
VM gets an hour before it may stop again, so remove the switch, as
[The nightly stop](#the-nightly-stop) says, when the work may take longer, and create it
again after. Never
apply between 09:25 and 16:15 ET on a session day, the window
[Replace the instance, and the approval window](#replace-the-instance-and-the-approval-window)
sets for CI's apply.

Once `infra/live/` describes the VM, a laptop plan also needs the two lines in
[Add the VM's two lines to `live.tfvars`](#add-the-vms-two-lines-to-livetfvars).

Create a worktree at `main`'s head, from the main checkout.

```bash
git fetch origin
```

```bash
git worktree add --detach "$HOME/marketlake-infra-main" origin/main
```

Check it. The two commands should print the same full commit id.

```bash
git -C "$HOME/marketlake-infra-main" rev-parse HEAD
```

```bash
gh api repos/l3a0/marketlake/commits/main --jq .sha
```

Initialize, plan and read the plan, then apply.

```bash
AWS_PROFILE=marketlake-admin tofu -chdir="$HOME/marketlake-infra-main/infra/live" init -backend-config="$HOME/.config/marketlake/infra/live.tfbackend"
```

```bash
AWS_PROFILE=marketlake-admin tofu -chdir="$HOME/marketlake-infra-main/infra/live" plan -var-file="$HOME/.config/marketlake/infra/live.tfvars"
```

```bash
AWS_PROFILE=marketlake-admin tofu -chdir="$HOME/marketlake-infra-main/infra/live" apply -var-file="$HOME/.config/marketlake/infra/live.tfvars"
```

When done, end the admin session as in step 12, and remove the worktree from the main
checkout.

```bash
git worktree remove "$HOME/marketlake-infra-main"
```

## Recovery

A change made in the console shows up as a difference in the next plan. Adopt it into
code or revert it. A manual run of `infra.yml` from `main` reverts it behind the same
approval, with no code change.

A failed apply keeps the state of what it changed and releases its lock, so the next run
plans only what is left. When a runner, the GitHub machine that runs the job, dies in the
middle of an apply, its lock file, `live/terraform.tfstate.tflock`, stays behind, and the
next run's error prints the lock's id. Clear it from an admin session, in a worktree at
`main`'s head made as in the section above. Initialize that worktree against
`live.tfbackend`.

```bash
AWS_PROFILE=marketlake-admin tofu -chdir="$HOME/marketlake-infra-main/infra/live" init -backend-config="$HOME/.config/marketlake/infra/live.tfbackend"
```

Then release the lock, with the id the error printed in place of `<lock-id>`.

```bash
AWS_PROFILE=marketlake-admin tofu -chdir="$HOME/marketlake-infra-main/infra/live" force-unlock "<lock-id>"
```

## Changing the bootstrap

Apply any later change to `infra/bootstrap/*.tf` from the laptop first, as in
step 7, then approve its live apply. The live apply needs the VM running, so start it
first if it is stopped, as in [Start a stopped instance](#start-a-stopped-instance). A
started VM gets an hour before it may stop again. Remove the switch, as
[The nightly stop](#the-nightly-stop) says, when the plan and the apply may take longer,
and create it again after. Apply it only from the bootstrap pull
request's branch at its final head, and only when its plan changes nothing that pull
request does not change. Every checkout reads the same backend file in
`~/.config/marketlake/infra/`, so a checkout that predates a merged change plans that
change away, and OpenTofu gives no warning. After the merge, plan the bootstrap from
`main`'s merge commit, as in step 11, and expect no changes.

### When the merge came before the bootstrap apply

A bootstrap pull request can merge before its bootstrap apply, as
[PR #719](https://github.com/l3a0/marketlake/pull/719) did on 2026-10-06. Its
`tofu apply (live)` run then waits for approval in the `infra` environment. Leave that
run waiting when its live change needs a permission the bootstrap change adds. The run
uses the apply role's old permissions, so it applies what the old role allows and fails
on each call it does not. Approved first,
[PR #719](https://github.com/l3a0/marketlake/pull/719)'s run would have created
`aws_iam_role_policy.instance_config_read` and failed at `iam:CreateUser`.

The rule above, to apply from the branch's final head, assumes the apply comes before
the merge. After the merge, apply the bootstrap from the merge commit instead, in this
order.

1. Read the pull request's final head and its merge commit. They are `<branch-head>` and
   `<merge-commit>` below.

   ```bash
   gh pr view "<n>" --repo l3a0/marketlake --json headRefOid,mergeCommit --jq '.headRefOid, .mergeCommit.oid'
   ```

2. Make the worktree at the merge commit, from the main checkout. A merge deletes the
   branch, so fetch the head from the pull request's own ref. When a worktree for this
   pull request already exists, remove it first, as in step 12.

   ```bash
   git fetch origin main "pull/<n>/head"
   ```

   ```bash
   git worktree add --detach "$HOME/marketlake-infra-<n>" "<merge-commit>"
   ```

3. Compare the bootstrap at the two commits. An empty diff means the merge commit carries
   the bootstrap the pull request reviewed. A difference comes from another merge, and the
   plan must still change nothing this pull request does not change.

   ```bash
   git -C "$HOME/marketlake-infra-<n>" diff "<branch-head>" "<merge-commit>" -- infra/bootstrap
   ```

4. Run step 7's `init`, `plan` and `apply` commands, and read the plan before the
   apply. Skip step 7's worktree and head checks, which the steps above replace.
5. Approve the waiting run, as in
   [step 11](#11-merge-approve-and-confirm-nothing-is-left-to-change).

A run approved before the bootstrap apply has failed, and [Recovery](#recovery)
covers what a failed apply leaves behind. Once the bootstrap is applied, start a new run
with step 11's `gh workflow run infra.yml --repo l3a0/marketlake --ref main`, and
approve it as in step 11.

### Reading a statement inserted into a policy

OpenTofu 1.13 shows a statement inserted into an inline policy's `Statement` list as a
change to the statement that held its slot. The statements after it can then show as
changed, removed and added again, though none of them changed. To check such a plan,
collect the old side of every entry shown as changed or removed, and the new side of
every entry shown as changed or added. Match the two sets by `Sid` and compare each
pair. When the only statement without a twin is the new one, the only real change is the
insertion.

A statement inserted just before the last one shows the shortest form of this. The last
statement's slot changes into the new statement, and the last statement is added again
at the end.

[PR #719](https://github.com/l3a0/marketlake/pull/719)'s bootstrap plan read
`Plan: 0 to add, 1 to change, 0 to destroy`. The pull request inserted
`TokenWriterUserWrite` before `Ec2InHomeRegion` in `aws_iam_role_policy.apply`. The plan
showed `Ec2InHomeRegion`'s slot changing into `TokenWriterUserWrite`, with its `Action`,
`Condition`, `Resource` and `Sid` all marked changed, and `Ec2InHomeRegion` added again
at the end, unchanged. Nothing about EC2 changed.

## The config parameters

The hosted VM's `config.yaml` is written at deploy time from SSM Parameter Store, so a VM
can be rebuilt and a secret rotated without logging in to it
([#699](https://github.com/l3a0/marketlake/issues/699)). Five `SecureString` parameters
sit under `/marketlake/config/`, the four secrets and the token. Each one but the token
is the `config.yaml` key it fills, written in kebab case.

1. `/marketlake/config/schwab-api-key`, for `schwab_api_key`.
2. `/marketlake/config/schwab-app-secret`, for `schwab_app_secret`.
3. `/marketlake/config/healthchecks-ping-key`, for `healthchecks_ping_key`.
4. `/marketlake/config/ntfy-topic`, for `ntfy_topic`.
5. `/marketlake/config/schwab-oauth-token`, the contents of `token.json`.

The VM's `backup_target` is not a parameter. It arrives as the instance tag
`marketlake:backup-target`, which `infra/live/vm.tf` sets to `s3://<backup-bucket>/lake`
from the bucket variable OpenTofu already holds, by the owner's decision of 2026-10-07
([#686](https://github.com/l3a0/marketlake/issues/686)). The instance serves its tags
through instance metadata, because `vm.tf` sets `instance_metadata_tags`, and the render
reads the tag there. So no hand step puts the bucket's name, and a bucket change reaches
the VM with the apply that changes the tag in place.

`python -m lake.vm_config render < config/vm.yaml` writes the VM's `config.yaml`. It
joins the four secret parameters and the tag with the tracked `config/vm.yaml`, which
holds the settings that are not secret. The token goes to `token.json` through its own
pull ([#636](https://github.com/l3a0/marketlake/issues/636)). A tag the metadata
service does not serve gives exit 3, which the first boot retries, because AWS does
not document that a tag given at launch is served from the first moment of the
first boot. The line names both causes. Either the tag is not served yet, or the
instance has no tag or has `instance_metadata_tags` disabled, which the owner fixes in
`vm.tf`. Metadata that does not answer also gives exit 3. Any other HTTP error from
it gives exit 1, and so does a redirect, which the render never follows. A tag value
that is empty, padded or not UTF-8 makes the render refuse with exit 2.

The code manages no parameter and names only the path. A parameter resource needs its
value at apply time, so CI would hold every secret, and a data source writes the decrypted
value into the state. So the owner puts each value from the laptop. `marketlake-instance`
reads the path, and neither CI role can. AWS's `AmazonSSMManagedInstanceCore`, which
[#695](https://github.com/l3a0/marketlake/issues/695) attaches, lets the instance role
read every parameter in the account, which holds no other secret. The role
`marketlake-token-writer` can only overwrite the token, and only `marketlake-command` may
assume it ([#737](https://github.com/l3a0/marketlake/issues/737)). The laptop's weekly
re-auth does that when its `config.yaml` sets `token_store: both`
([#636](https://github.com/l3a0/marketlake/issues/636)).

### Create the token writer, in order

Since [#737](https://github.com/l3a0/marketlake/issues/737) the token writer is a role,
and the live apply creates it together with the role `marketlake-backup` and the laptop's
one user, `marketlake-command`, which assumes both. This heading keeps its old name so
that links to it still work. The pull request that added them changed the bootstrap too,
so it follows [Changing the bootstrap](#changing-the-bootstrap).

1. Apply the bootstrap from the laptop, from the pull request's branch at its final head,
   as in step 7. That gives the apply role `iam:CreateUser` and `iam:PutUserPolicy` on
   `marketlake-command`, and `iam:CreateRole` and `iam:PutRolePolicy` on the two roles.
   When the pull request merged first, follow
   [When the merge came before the bootstrap apply](#when-the-merge-came-before-the-bootstrap-apply)
   in place of this step and the next.
2. Merge, then approve the `tofu apply (live)` run, as in
   [step 11](#11-merge-approve-and-confirm-nothing-is-left-to-change), which creates the
   user, the two roles and their policies. That apply's plan summary also showed four
   `forget` rows, one for each old user and its policy, which left the state and kept
   existing in AWS. Since [#741](https://github.com/l3a0/marketlake/issues/741) deleted
   those `removed` blocks, a plan shows none.
   Every approved apply carries everything on `main`, so refuse a run whose plan summary
   shows `aws_instance.vm` or `aws_ebs_volume.lake`. That run would boot the hosted VM
   early. Merge and apply this before the hosted VM's pull request merges, or hold that
   pull request until this apply has run.
3. Wait until [#737](https://github.com/l3a0/marketlake/issues/737)'s code pull request
   has merged and `main` is deployed to the laptop, because no earlier code reads the keys
   below. Then create an access key for `marketlake-command` in the AWS console. The key goes into the
   laptop's `config.yaml` as `command_access_key_id` and `command_secret_access_key`, and
   it stays out of code, so no secret reaches the state.
4. Write each role's ARN into the laptop's `config.yaml`, `marketlake-backup`'s as
   `bucket_role_arn` and `marketlake-token-writer`'s as `token_store_role_arn`. An ARN
   carries the account id, so each command below writes it straight into a copy of the
   file and prints nothing, and the account id reaches no transcript or log. The copy is
   renamed into place at the end, because capture reads `config.yaml` every minute and a
   half-written file would stop it. Make the copy.

   ```bash
   cp -p "$HOME/.config/marketlake/config.yaml" "$HOME/.config/marketlake/config.yaml.new"
   ```

   Add the backup role's ARN.

   ```bash
   A=$(aws iam get-role --role-name marketlake-backup --query Role.Arn --output text --profile marketlake-admin) && printf '\nbucket_role_arn: %s\n' "$A" >> "$HOME/.config/marketlake/config.yaml.new"; unset A
   ```

   Add the token writer's ARN.

   ```bash
   A=$(aws iam get-role --role-name marketlake-token-writer --query Role.Arn --output text --profile marketlake-admin) && printf '\ntoken_store_role_arn: %s\n' "$A" >> "$HOME/.config/marketlake/config.yaml.new"; unset A
   ```

   Each line starts with a newline, because a copy whose last line has none would
   otherwise join the new key to it and stop `config.yaml` from loading. A `get-role` that
   fails appends nothing, so check that both keys landed before going on.

   ```bash
   grep -q '^bucket_role_arn:' "$HOME/.config/marketlake/config.yaml.new" && grep -q '^token_store_role_arn:' "$HOME/.config/marketlake/config.yaml.new" && echo both-present
   ```

   Finish the copy's other settings, which
   [#737](https://github.com/l3a0/marketlake/issues/737)'s step 6 lists, then rename it
   into place only once the check printed `both-present`.

   ```bash
   mv "$HOME/.config/marketlake/config.yaml.new" "$HOME/.config/marketlake/config.yaml"
   ```

Never create the user or either role by hand. The live apply creates them, and its
`CreateUser` or `CreateRole` fails with `EntityAlreadyExists` when one already exists.

A rebuild into a fresh account puts all five values below before its first live apply,
because that apply also creates the VM, and the VM's first boot reads them. The bucket's
name needs no put, because the same apply sets the instance tag that carries it. The puts
need only the admin session. The rebuild creates `marketlake-command`'s key and reads
both ARNs after that live apply.

### Move the laptop onto `marketlake-command`

Before [#737](https://github.com/l3a0/marketlake/issues/737), the laptop's AWS key
belonged to the user `marketlake-backup`, and the user `marketlake-token-writer` never got
one. The move runs once, around the merge of
[#737](https://github.com/l3a0/marketlake/issues/737)'s infra pull request.
[#737](https://github.com/l3a0/marketlake/issues/737)'s body carries every step and its
rollback, and this section carries the commands for three of them.

#### Probe the name rule

Before the bootstrap apply, check that a user and a role may share a name, because the
role `marketlake-backup` arrives while the user `marketlake-backup` still exists. AWS's
documentation is ambiguous here. If they cannot, the live apply's `CreateRole` fails after
the merge, and the roles need new names before merging. The probe creates a user and a
role under one throwaway name, then deletes both. The commands are joined with `;`, so both
deletes run even when a create fails.

Never use a real role name for the probe. A role made by hand makes the live apply's
`CreateRole` fail with `EntityAlreadyExists`, and the apply role may not delete it.

```bash
aws iam create-user --user-name marketlake-name-probe --query User.UserName --output text --profile marketlake-admin; aws iam create-role --role-name marketlake-name-probe --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' --query Role.RoleName --output text --profile marketlake-admin; aws iam delete-role --role-name marketlake-name-probe --profile marketlake-admin; aws iam delete-user --user-name marketlake-name-probe --profile marketlake-admin
```

A `create-role` that prints the new role means the names may be shared. One that fails
with `EntityAlreadyExists` means they may not. Its `delete-role` then fails with
`NoSuchEntity`, which is expected.

#### Look the calls up in CloudTrail

After the laptop's bucket `live-check` and a re-auth have run under the roles, look their
calls up in CloudTrail's event history, allowing about 5 minutes for delivery. Event
history records management calls only, so it shows each `AssumeRole`,
`GetBucketVersioning` and `PutParameter`, and never an object read or write. The command
prints each event's time, its name, the caller's ARN with its account prefix stripped, and
for an `AssumeRole` the session name. It never prints `roleArn`, which carries the account
id. It sets a start time rather than a result limit, because a limit turns off the CLI's
paging, and the instance profile's own `AssumeRole` events could fill the one page.

```bash
for e in AssumeRole GetBucketVersioning PutParameter; do aws cloudtrail lookup-events --region us-east-1 --profile marketlake-admin --lookup-attributes "AttributeKey=EventName,AttributeValue=$e" --start-time "<switch-time>" --output json | jq -r '.Events[].CloudTrailEvent | fromjson | [.eventTime, .eventName, ((.userIdentity.arn // "-") | sub("^arn:aws:[a-z]+::[0-9]+:"; "")), (.requestParameters.roleSessionName? // "-")] | @tsv'; done
```

The calls to look for are these three.

1. `AssumeRole` by `user/marketlake-command`, once with the session name
   `marketlake-bucket` and once with `marketlake-token-put`.
2. `GetBucketVersioning` by `assumed-role/marketlake-backup/marketlake-bucket`.
3. `PutParameter` by `assumed-role/marketlake-token-writer/marketlake-token-put`.

The bucket's session is assumed through STS in the bucket client's region, and S3 records
its calls in the bucket's region. When `bucket_region` is not `us-east-1`, run the command
again with `--region` set to it, to find the `AssumeRole` for `marketlake-bucket` and the
`GetBucketVersioning`.

#### Retire the old users

Delete the old users only once three things hold.

1. The bucket check and the re-auth above both passed.
2. A nightly upload has run under the role, if the laptop's backup target is the bucket by
   then.
3. `marketlake-backup`'s key was last used before its last copy was removed, and a
   nightly cycle has passed since, by the check below.

CloudTrail's event history never shows an object call, so the key's last use is the only
record that the nightly job, and the copy of the key that
[#686](https://github.com/l3a0/marketlake/issues/686) put on the measurement VM, have
stopped using it. First remove the `bucket_*` key values from the laptop's `config.yaml`
and from the measurement VM, or terminate that VM.

Read the old key's id. It is `<backup-key-id>` below.

```bash
aws iam list-access-keys --user-name marketlake-backup --profile marketlake-admin
```

Read when it was last used. Go on only when `LastUsedDate` is earlier than the removal of
the key's last copy, from the laptop or the measurement VM, and a nightly cycle has passed
since that removal. A use after the switch but before the removal does not block the
gate.

```bash
aws iam get-access-key-last-used --access-key-id "<backup-key-id>" --profile marketlake-admin
```

Delete the key.

```bash
aws iam delete-access-key --user-name marketlake-backup --access-key-id "<backup-key-id>" --profile marketlake-admin
```

List the user's inline policies. The one name it prints is `<policy-name>`.

```bash
aws iam list-user-policies --user-name marketlake-backup --profile marketlake-admin
```

```bash
aws iam delete-user-policy --user-name marketlake-backup --policy-name "<policy-name>" --profile marketlake-admin
```

`delete-user` fails with `DeleteConflict` while the user keeps an access key, a policy, a
group, a console password or an MFA device. The first four commands below should each
print an empty list, and the fifth should fail with `NoSuchEntity`, which means the user
has no console password.

```bash
aws iam list-access-keys --user-name marketlake-backup --profile marketlake-admin
aws iam list-attached-user-policies --user-name marketlake-backup --profile marketlake-admin
aws iam list-groups-for-user --user-name marketlake-backup --profile marketlake-admin
aws iam list-mfa-devices --user-name marketlake-backup --profile marketlake-admin
aws iam get-login-profile --user-name marketlake-backup --profile marketlake-admin
```

```bash
aws iam delete-user --user-name marketlake-backup --profile marketlake-admin
```

The user `marketlake-token-writer` holds no key, and its one policy is
`put-schwab-oauth-token`.

```bash
aws iam delete-user-policy --user-name marketlake-token-writer --policy-name put-schwab-oauth-token --profile marketlake-admin
```

Run the same five checks for it.

```bash
aws iam list-access-keys --user-name marketlake-token-writer --profile marketlake-admin
aws iam list-attached-user-policies --user-name marketlake-token-writer --profile marketlake-admin
aws iam list-groups-for-user --user-name marketlake-token-writer --profile marketlake-admin
aws iam list-mfa-devices --user-name marketlake-token-writer --profile marketlake-admin
aws iam get-login-profile --user-name marketlake-token-writer --profile marketlake-admin
```

```bash
aws iam delete-user --user-name marketlake-token-writer --profile marketlake-admin
```

The IAM console's delete of a user is the alternative to these commands. It removes the
user's keys, policies and other attachments before the user itself. It still waits for
the `get-access-key-last-used` check above.

### Put the values

Every command below passes `--profile marketlake-admin` and `--region us-east-1`. The CI
roles' Deny and the VM's read both name that region, so a parameter put anywhere else is
readable by the plan role and out of the VM's reach. Each passes `--tier Standard`,
because a parameter once put as Advanced never moves back to Standard, and
[#636](https://github.com/l3a0/marketlake/issues/636)'s re-auth puts the token as
Standard, so its puts would then fail. Each also passes `--overwrite`, so a re-run
replaces the value instead of failing with `ParameterAlreadyExists`.

Each of the four secrets is read from the keyboard into a variable.
A value typed on the command line lands in shell history. A value kept in a file picks up
the editor's trailing newline, and the put stores that newline as part of the secret.
`read -rs` echoes nothing and writes nothing to disk, and `unset` drops the value after
the put. The value is piped into the put and read from `file:///dev/stdin`, so it never
appears in any command's arguments. `ps` shows every argument of a running command, and on
macOS it shows them across users. `printf '%s'` adds no newline. Passing `--value "$V"`
would also fail with `expected one argument` for a value that starts with `-`, and would
read a value that starts with `file://` as a path. Each command prints a prompt. Paste the
value and press Enter. The prompt comes from `printf`, because zsh's `read -p` means a
coprocess and leaves the variable empty. The variable lives in one command line, because a
command run through Claude Code's `!` prefix may start a fresh shell.

```bash
printf 'Schwab API key: '; read -rs V && echo && printf '%s' "$V" | aws ssm put-parameter --name /marketlake/config/schwab-api-key --value file:///dev/stdin --type SecureString --tier Standard --overwrite --profile marketlake-admin --region us-east-1; unset V
```

```bash
printf 'Schwab app secret: '; read -rs V && echo && printf '%s' "$V" | aws ssm put-parameter --name /marketlake/config/schwab-app-secret --value file:///dev/stdin --type SecureString --tier Standard --overwrite --profile marketlake-admin --region us-east-1; unset V
```

```bash
printf 'Healthchecks ping key: '; read -rs V && echo && printf '%s' "$V" | aws ssm put-parameter --name /marketlake/config/healthchecks-ping-key --value file:///dev/stdin --type SecureString --tier Standard --overwrite --profile marketlake-admin --region us-east-1; unset V
```

```bash
printf 'ntfy topic: '; read -rs V && echo && printf '%s' "$V" | aws ssm put-parameter --name /marketlake/config/ntfy-topic --value file:///dev/stdin --type SecureString --tier Standard --overwrite --profile marketlake-admin --region us-east-1; unset V
```

The backup target has no put. The instance tag in
[The config parameters](#the-config-parameters) carries it.

The token comes from its file, which `src/lake/reauth.py`'s `write_token` writes with no
trailing newline. Skip this put when
[#636](https://github.com/l3a0/marketlake/issues/636)'s re-auth has already written the
token.

```bash
aws ssm put-parameter --name /marketlake/config/schwab-oauth-token --value "file://$HOME/.config/marketlake/token.json" --type SecureString --tier Standard --overwrite --profile marketlake-admin --region us-east-1
```

Optionally, check that all five landed. The listing shows each parameter's name, type
and tier, and never a value.

```bash
aws ssm describe-parameters --profile marketlake-admin --region us-east-1 --parameter-filters "Key=Path,Values=/marketlake/config"
```

### Recover from a leaked `marketlake-command` key

The laptop's one key reaches both roles, so a leaked key can write the token and read and
overwrite objects in the bucket. Versioning keeps the copy each overwrite replaced, so the
bucket's recovery is the version recovery by hand that `README.md` describes. The token
needs more. A put can change more than the value. It can move the parameter to the
Advanced tier, attach a parameter policy, set an allowed pattern, or store a forged token
that the VM's pull accepts. So recovery deletes the parameter whatever its listing shows,
and a fresh re-auth puts the real token back.

1. Deactivate the key for `marketlake-command` in the AWS console. Deactivating stops the
   key itself and nothing it already minted. A role session issued before it keeps working
   for up to an hour, the roles' session limit. A session token from `sts:GetSessionToken`,
   which needs no permission to mint, lasts up to 36 hours and can still assume both roles.
2. Revoke what the key already minted, in the admin session. Deny the user every request
   signed with a token issued before now. A request signed with an access key carries no
   token issue time, so the new key in step 3 is unaffected.

   ```bash
   aws iam put-user-policy --user-name marketlake-command --policy-name revoke-older-sessions --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Deny\",\"Action\":\"*\",\"Resource\":\"*\",\"Condition\":{\"DateLessThan\":{\"aws:TokenIssueTime\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}}]}" --profile marketlake-admin
   ```

   Then, on each of the roles `marketlake-backup` and `marketlake-token-writer`, open the
   console's **Revoke sessions** tab and revoke active sessions, which denies every role
   session issued before then. Neither policy is one the live apply manages, so the apply
   leaves both alone. Delete `revoke-older-sessions` from the user after 36 hours.
3. Create a new access key for the same user, and put it in the laptop's `config.yaml` as
   `command_access_key_id` and `command_secret_access_key`.
4. Delete the parameter.

   ```bash
   aws ssm delete-parameter --name /marketlake/config/schwab-oauth-token --profile marketlake-admin --region us-east-1
   ```

5. Run the weekly re-auth, the rendered `reauth.sh`, which creates the parameter again as
   a Standard `SecureString`. While the laptop's `config.yaml` leaves `token_store` absent
   or `file`, the re-auth puts nothing, so put the token again with the `file://` command
   above instead.
6. Delete the deactivated key in the console. A user holds at most two access keys, and a
   deactivated key counts toward the two.

Step 4 mints a new token rather than putting the laptop's existing `token.json` again. A
forged token can carry a later mint time than that file, and the VM's pull never writes
an older mint over a newer one. The guard against a forged token belongs to
[#636](https://github.com/l3a0/marketlake/issues/636)'s pull, which refuses a mint time
more than an hour past its own clock. So if capture has not recovered, re-auth once more
after that hour.

## The hosted VM

The VM that captures for the hosted deployment is code in `infra/live/vm.tf`
([#686](https://github.com/l3a0/marketlake/issues/686)), so a rebuild after a dead VM, a
resize or a restore is one reviewed apply. cloud-init, the tool that runs an instance's
first-boot script, takes each new VM from nothing to a running daemon with no login. This
section holds what the code cannot do: the owner's steps before the first apply, and the
hand steps after one. The design's "Infrastructure, defined" carries the reasoning.

Every `aws` command below runs in an admin session, as in
[step 2](#2-open-an-admin-session-under-a-named-profile), and passes
`--profile marketlake-admin` and `--region us-east-1`. Commands shown as run on the VM
run in an SSH session as `ubuntu`, Ubuntu's default account, which owns the checkout and
runs the daemon.

### The owner's steps, in order

1. **Check the five parameters.** The four secrets and the token each sit in their
   `SecureString` parameter, with the commands in [Put the values](#put-the-values). The
   VM's render refuses to write `config.yaml` while a secret is missing. The bucket's name
   needs no put, because the apply sets the `marketlake:backup-target` tag that carries
   it. In a rebuild into a fresh account, every parameter goes in before the first live
   apply, as [Create the token writer, in order](#create-the-token-writer-in-order) says.
2. **Make the token's weekly put work before the first boot.** Done on 2026-10-07. The
   owner put `/marketlake/config/schwab-oauth-token` at version 1 on 2026-10-06.
   [#737](https://github.com/l3a0/marketlake/issues/737)'s pull requests,
   [PR #743](https://github.com/l3a0/marketlake/pull/743) and
   [PR #744](https://github.com/l3a0/marketlake/pull/744), merged on 2026-10-07, and
   [PR #743](https://github.com/l3a0/marketlake/pull/743)'s live apply ran the same day.
   The laptop then moved onto `marketlake-command`, with `bucket_credentials:
   assume_role` and `token_store: both` in its `config.yaml`, and its re-auth put version
   2 of the token through the role `marketlake-token-writer`. So the weekly put reaches
   the parameter with no key for a token writer user. What remains of
   [#737](https://github.com/l3a0/marketlake/issues/737) is its step 8, deleting the old
   users once their keys go idle, which the VM does not wait on. The VM only pulls the
   token, through its instance role, so
   [#737](https://github.com/l3a0/marketlake/issues/737) changes nothing on the VM.

   The order of the merges matters.
   [#737](https://github.com/l3a0/marketlake/issues/737)'s infra pull request,
   [PR #743](https://github.com/l3a0/marketlake/pull/743), merged on 2026-10-07, and its
   apply ran the same day, so the rule that it run before
   [PR #742](https://github.com/l3a0/marketlake/pull/742) merges already holds. An
   approved `infra` apply carries all of `main`, so once
   [PR #742](https://github.com/l3a0/marketlake/pull/742) is on `main`, any approved
   apply, [#737](https://github.com/l3a0/marketlake/issues/737)'s included, creates the
   VM and starts its first boot.
3. **Set the two CI inputs before the VM's pull request is planned.** The owner set the
   `OWNER_SSH_CIDR` secret and the `SSH_PUBLIC_KEY` variable on 2026-10-07, the variable
   from the existing `~/.ssh/marketlake_vm.pub`. A rebuild sets them with the commands
   in [step 9](#9-set-the-secrets-and-the-variables). Until both are set, the pull
   request's `plan` job refuses, and its red result waits on the owner, since `plan` is
   not a required check. A merge with either one unset applies nothing, because the
   `apply` job's refusal exits before `init`. Re-running that apply after both are set
   applies. CI reads only the secret and the variable, so nothing else is needed before
   the first apply.
4. **Check the account.** The account needs a default VPC with a default subnet in
   `us-east-1c`, and no key pair or security group named `marketlake-vm`.
   [The duplicate-name check](#the-duplicate-name-check) covers the names. This command
   prints the default subnet's id, and prints nothing when the account has no default
   VPC or no default subnet in that zone.

   ```bash
   aws ec2 describe-subnets --filters Name=default-for-az,Values=true Name=availability-zone,Values=us-east-1c --query 'Subnets[].SubnetId' --output text --profile marketlake-admin --region us-east-1
   ```

   Without a default VPC, the pull request's plan fails at the `data "aws_vpc"` lookup,
   before anything is created. The zone is a literal in the code. The measurement VM,
   `marketlake-measure`, runs in it, so the zone is known to offer `t4g.small` in this
   account.

   The image lookup in `infra/live/vm.tf` names Ubuntu's images by a pattern. On
   2026-10-07 this command printed `ubuntu-noble-24.04-arm64-server-20261004`, so the
   pattern finds an image. Run it again before a later apply if Canonical's naming may
   have changed. The command prints the newest matching image's name.

   ```bash
   aws ec2 describe-images --owners 099720109477 --filters Name=name,Values='ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-arm64-server-*' --query 'sort_by(Images,&CreationDate)[-1].Name' --output text --profile marketlake-admin --region us-east-1
   ```

   `None`, or an error, means the pattern matches no image. Then the plan fails at the
   `data "aws_ami"` lookup, and the pattern in `vm.tf` needs a reviewed fix.
5. **Approve the apply on the evening before
   [#638](https://github.com/l3a0/marketlake/issues/638)'s shadow day**, outside
   [the approval window](#replace-the-instance-and-the-approval-window). Two things set
   that time.
   1. The VM captures from its first boot. The owner's waiver on
      [#638](https://github.com/l3a0/marketlake/issues/638) accepts the chain windows the
      laptop loses to Schwab's rate limit while both hosts capture, about 27 a session,
      for the shadow day only. Every earlier session the VM runs costs the laptop the
      same.
   2. The VM runs the commit it cloned at first boot, and nothing pulls new code onto it
      until a deploy runs, as [Deploy a commit](#deploy-a-commit) says. So the apply
      follows the merges of the two blockers of
      [#638](https://github.com/l3a0/marketlake/issues/638) that change code the VM runs:
      [#702](https://github.com/l3a0/marketlake/issues/702), and
      [#669](https://github.com/l3a0/marketlake/issues/669)'s
      [PR #731](https://github.com/l3a0/marketlake/pull/731). Both have merged:
      [PR #731](https://github.com/l3a0/marketlake/pull/731) on 2026-10-07, and
      [#702](https://github.com/l3a0/marketlake/issues/702)'s code in
      [PR #750](https://github.com/l3a0/marketlake/pull/750) and
      [PR #751](https://github.com/l3a0/marketlake/pull/751) the same day. With
      [#702](https://github.com/l3a0/marketlake/issues/702) on the VM from its first boot,
      the VM pulls each weekly token on its own. A merge after the apply reaches the VM
      only through a deploy, as [Deploy a commit](#deploy-a-commit) describes.

   Whether the VM keeps capturing between the shadow day and the cutover is
   [#638](https://github.com/l3a0/marketlake/issues/638)'s call.
6. **Terminate `marketlake-measure` once the new VM runs.** The owner installed it by
   hand, and the code does not import it.

A home address that changes locks SSH out until an approved apply updates the rule. Set
the secret again with step 9's command, update the line in `live.tfvars`, start a manual
run with `gh workflow run infra.yml --repo l3a0/marketlake --ref main`, and approve it
outside the window.

### Create the VM

The approved apply of the VM's pull request creates the VM. The pull request's plan
summary shows these six creates, and no destroy or replace. The owner confirmed the table
on 2026-10-07 in the step summary of the pull request's plan at `fcd37f9`, the first plan
to run past the check for empty inputs. The count comes from the code: the five
resources in `vm.tf`, and the read half from the instance role's split. The
`marketlake:backup-target` tag is an argument of `aws_instance.vm`, so it adds no
resource.

| Address | Action |
| --- | --- |
| `aws_ebs_volume.lake` | create |
| `aws_iam_role_policy.instance_s3_read` | create |
| `aws_instance.vm` | create |
| `aws_key_pair.vm` | create |
| `aws_security_group.vm` | create |
| `aws_volume_attachment.lake` | create |

The read half of the instance role's S3 policy is the one IAM change. The write half,
`s3:PutObject` alone, stayed off until
[#638](https://github.com/l3a0/marketlake/issues/638)'s cutover turned it on on 2026-10-08.

At first boot, cloud-init runs the shim that `infra/live/user-data.sh.tftpl` renders. A
shim here is a short script whose only job is to hand over to the tracked bootstrap. It
is the instance's `user_data`, and it carries nothing secret, because anyone who can
describe the instance can read it. The boot takes two steps.

1. The shim writes the owner and the lake volume's id to
   `/etc/marketlake/bootstrap.conf`, clones `main` into `~/marketlake` as the owner, with
   up to 10 attempts 30 seconds apart, and runs `deploy/vm-bootstrap.sh`.
2. The bootstrap mounts the lake volume at `/srv/marketlake`, formatting it only when
   `blkid` finds no filesystem and no ext4 superblock sits where the bootstrap's own
   `mkfs` puts one. It writes the volume's `/etc/fstab` line, and stops when it
   cannot read the old file or when the new one would hold no entry for `/`. It installs
   the `uv` version `.tool-versions` pins, with a download that `curl` retries up to 5
   times on any error. Then it syncs the environment with
   `deploy/linux-install.sh --sync-only`, which starts nothing. From that environment it
   renders `config.yaml` from `config/vm.yaml`, the parameters and the tag, pulls the
   token, and applies the roster. It takes the install lock around each attempt of
   these steps and releases it during the 20 seconds it waits before a retry. One
   attempt against an SSM endpoint that hangs holds the lock about 160 seconds, four
   botocore attempts of up to 40 seconds each, well under the 600 seconds another
   install waits for it. A metadata read cannot hang that long, because each request
   to the metadata service times out after a second. Last, it runs
   `deploy/linux-install.sh` again without the flag, which installs and starts the
   units, so the residents start with their config already in place. That install runs
   even when a config step failed.

Each line the bootstrap prints starts `vm-bootstrap:`, and a run that finished prints
`vm-bootstrap: done` last. A failed first boot shows in `/var/log/cloud-init-output.log`
and the journal, not in healthchecks. A VM runs as the `role` that `config/vm.yaml` names.
As `primary`, which it has been since [#638](https://github.com/l3a0/marketlake/issues/638)'s
cutover, its pings reach healthchecks. As `shadow`, they go to its own `journal/outbox/`
instead.

Read the first boot on the apply's evening. Find the address as in
[Find the VM's address](#find-the-vms-address), then open a session from the laptop.

```bash
ssh -i ~/.ssh/marketlake_vm ubuntu@"<vm-address>"
```

On the VM, cloud-init's summary says `status: done` for a boot whose script exited 0.

```bash
cloud-init status --long
```

Each step prints lines that start with its own name, and a step that refuses says why
on one of them. The bootstrap's lines start `vm-bootstrap:`, and each install's lines start
`linux-install:`. The config steps print `vm_config:`, `token_store:` and `roster:`
lines. Inside the last install, the render of the units prints `render:` lines and the
rendered `install.sh` prints `install.sh:` lines. This search shows every one of them.

```bash
grep -E '^(vm-bootstrap|linux-install|render|install\.sh|vm_config|token_store|roster): ' /var/log/cloud-init-output.log
```

The rendered `install.sh` also prints lines that carry no step's name. Before each
command it runs, it prints the command on a line that starts `+`, and it closes by
reading back each resident's state on indented lines such as `  ActiveState=active`.
The search above leaves those out, so print the log from the line that starts the last
install to the end. That shows the last install whole, its read-back included, and the
bootstrap's closing line.

```bash
sed -n '/^vm-bootstrap: installing and starting the units/,$p' /var/log/cloud-init-output.log
```

These reads assume `config/vm.yaml` says `role: shadow`, as it did for the first boots in
[#686](https://github.com/l3a0/marketlake/issues/686). Since
[#638](https://github.com/l3a0/marketlake/issues/638)'s cutover it says `primary`. A primary
on an empty lake refuses its roster, and its daemon exits 2 at start, so a VM built from
scratch as primary needs [Restore the lake](#restore-the-lake) before these reads mean
anything.

The daemon prints its role at every start, so its journal should hold `daemon: role=` and
the role `config/vm.yaml` names.

```bash
TZ=America/New_York journalctl -u com.marketlake.daemon | grep 'role='
```

The daemon writes `journal/metadata.json` every idle minute, so its time should move
between two runs a minute apart.

```bash
stat -c %y /srv/marketlake/journal/metadata.json
```

The root volume is 16 GiB, which `df -h` reported as 15G. On 2026-10-09, two days after the
first boot, 2.9G of it was used. `uv`'s cache held 391M and the venv 28M of that.

```bash
df -h /
```

```bash
du -sh ~/.cache/uv ~/marketlake/.venv
```

The token file lives on the root volume too, so a full or broken root volume fails the
token's write each time the access token is refreshed, about every 25 minutes. Capture
goes on, because the daemon keeps the refreshed token in memory and retries the write on
every cycle ([#860](https://github.com/l3a0/marketlake/issues/860)). If Schwab ever rotated
the refresh token on a refresh, which it did not when measured, the refreshing request
would still fail about every 25 minutes, with a line saying the refresh token rotated. The daemon prints one
line when that starts, naming the errno and the path, and another when the file catches
up. A full disk may keep the first line from being stored at all, so search for both.

```bash
TZ=America/New_York journalctl -u com.marketlake.daemon | grep 'token file'
```

The errno says what to repair. `ENOSPC` means free space on `/`, and `EROFS` or `EIO`
mean the filesystem needs repair. Hold deploys and restarts until the recovery line
prints. That line says the token file now holds a token at least as new as the
process's. A restart drops the token the daemon holds, so it starts again from the stale
file. That costs one refresh, or a re-auth after a line saying the refresh token rotated.

At the next open, data segments appear under `/srv/marketlake`. A shadow's pings land as
lines in the outbox under `/srv/marketlake/journal/outbox/`, and a primary's reach
healthchecks instead. The build plan lists both reads as a
live check, beside the replacement in
[Replace the instance](#replace-the-instance-and-the-approval-window), which runs on the
same evening as the first boot.

### Rerun the bootstrap

`deploy/vm-bootstrap.sh` can run again at any time, as root, over SSH. Each step skips
work that is already done, so a second run under the same conditions changes nothing.
Start the VM first if it is stopped, as in
[Start a stopped instance](#start-a-stopped-instance), and remove the switch that lets it
stop, as [The nightly stop](#the-nightly-stop) says. Create the switch again once the run
has ended.

```bash
sudo ~/marketlake/deploy/vm-bootstrap.sh
```

Each failure prints one line naming the step, and the exit code says how the run ended.

1. Exit 0 means every step passed.
2. Exit 2 means the bootstrap itself refused, which happens only before the first
   install, such as on a bad `bootstrap.conf` or an `/etc/fstab` with no entry for `/`.
   It also refuses three states of the lake volume, and formats nothing in any of them.
   1. The volume holds something other than ext4.
   2. A read of 4 KiB block 0 or 32768 failed or came back short.
   3. `blkid` finds no filesystem, but an ext4 superblock sits in block 0 or 32768. The
      line prints a read-only `e2fsck -n` command, and
      [When the lake volume holds an unreadable ext4](#when-the-lake-volume-holds-an-unreadable-ext4)
      says what to do.
3. Exit 1 covers every failure of a disk step or an install. A disk step that fails
   stops the run at once, and so does the first install, the one that syncs the
   environment, even when it refused with its own exit 2. The last install, the one
   that starts the units, runs whatever the config steps returned. When it fails, its
   line says whether a rerun installs and starts the units, and the run ends with exit 1.
4. Exit 4 means the last install installed and started the units, but a render, token
   pull or roster apply failed, even when the step itself refused with its own exit 2.
   Each of those skips only the steps that need it. A deploy reads exit 4 as units in
   place, so it still restarts the daemon when a restart is owed, and it writes no record
   of the deploy until a run passes every step.

So read every line, not only the last.

Three things call for a rerun.

1. **A parameter or the tag was missing at first boot.** A missing parameter makes
   the render refuse, and a missing tag makes it exit 3 on all six attempts. Either
   way the bootstrap ended with exit 4, and the daemon restarts every ten seconds
   without a `config.yaml`. Put a missing parameter, or fix a missing tag in
   `infra/live/vm.tf` through an approved apply, as the render's line says. Then rerun.
2. **Code merged after the apply.** A merge reaches the VM through
   [Deploy from CI](#deploy-from-ci) once the owner approves its run, or by hand, as
   [Deploy a commit](#deploy-a-commit) says. The deploy runs the bootstrap and restarts
   the daemon when it is owed, because the install restarts nothing. Timers start fresh
   processes and pick up new code by themselves.

3. **A larger lake volume.** Raise `lake_volume_gib` in a reviewed pull request. The
   apply grows the volume in place, and nothing grows the ext4 filesystem on it until the
   bootstrap's `resize2fs` runs. So rerun the bootstrap after the apply, then check the
   size.

   ```bash
   df -h /srv/marketlake
   ```

   `resize2fs` prints `Nothing to do!` while the kernel still sees the old size, so a
   size that has not changed means rerun a minute later. AWS cannot shrink a volume, so
   the size only grows.

   When the volume filled before it grew, the daemon may have lost minutes it fetched.
   Each segment the disk refused printed one line to journald, which lives on the root
   volume, and the line says whether a gap row landed for it
   ([#769](https://github.com/l3a0/marketlake/issues/769)). Find them this way.

   ```bash
   TZ=America/New_York journalctl -u com.marketlake.daemon | grep 'segment write failed'
   ```

   A gap row that did not land with a `FileExistsError` found a file already at the
   path. That is usually a segment whose rows were durable and whose end-of-stream
   marker failed, so its minute is captured and needs nothing. Any other minute whose
   gap row did not land is recorded only by a restart, which marks it `daemon_dead`,
   and only before compaction next seals that day. The bootstrap's install restarts
   nothing, so restart the daemon once the volume has grown.

   ```bash
   sudo ~/.local/state/marketlake/systemd/restart.sh daemon
   ```

**When the clone failed.** A clone that fails all 10 attempts leaves no bootstrap on
disk, so nothing ran. `cloud-init status --long` reports the boot as an error, and the
log's last line says the clone failed. The shim wrote `bootstrap.conf` before the clone,
so clone by hand as the owner, then run the bootstrap.

```bash
git clone --branch main https://github.com/l3a0/marketlake.git ~/marketlake
```

```bash
sudo ~/marketlake/deploy/vm-bootstrap.sh
```

A clone cut off partway can leave `~/marketlake` with no valid `HEAD`, and then the
clone refuses because the directory exists. When
`git -C ~/marketlake rev-parse --verify HEAD` fails, delete `~/marketlake` and clone
again.

#### When the lake volume holds an unreadable ext4

The bootstrap refuses with exit 2 when `blkid` finds no filesystem on the lake volume but
the ext4 magic sits in 4 KiB block 0 or 32768. The refusal names the block and the
device. Three things leave a volume that way.

1. A lake whose primary superblock is damaged.
2. An earlier `mkfs` that was interrupted after it wrote the backup superblocks.
   `ext2fs_flush` writes the backups before the primary.
3. A chance match in a fresh volume's random content, about once in 32,768 volumes.

Never run `mkfs.ext4` on this volume, because it erases the volume. The first two
causes hold an ext4 that `e2fsck` can repair. Check the volume read-only first. `-n`
opens it read-only and answers no to every question.

```bash
sudo e2fsck -n <dev>
```

When the primary superblock has a bad magic, fails its checksum or is corrupt, `e2fsck`
looks for group 1's backup on its own and prints `Superblock invalid, trying backup
blocks...`. It skips that search when the primary fails in other ways, such as a
revision too high to read. So read the backup directly as well.

```bash
sudo e2fsck -n -b 32768 -B 4096 <dev>
```

1. **Either check finds a filesystem.** It runs its passes, starting with
   `Pass 1: Checking inodes, blocks, and sizes`, and lists what it would fix. The volume
   holds ext4, a damaged lake or an interrupted `mkfs`. Repair the primary from group 1's
   backup, then rerun the bootstrap.

   ```bash
   sudo e2fsck -b 32768 -B 4096 <dev>
   ```

2. **Both checks find none.** Each prints that the superblock could not be read or does
   not describe a valid ext2/ext3/ext4 filesystem. The bytes the guard matched describe no
   ext4 that `e2fsck` can open, which on a fresh volume is the chance match. Only then
   clear the block the refusal named, then rerun the bootstrap. This writes 4 KiB of zeros
   to the volume. If the volume is known to have held a lake, stop and investigate
   instead.

   ```bash
   sudo dd if=/dev/zero of=<dev> bs=4096 seek=<block> count=1 conv=fsync
   ```

### Deploy a commit

A merge to `main` reaches the VM only through a deploy, and a deploy at the wrong moment
loses captured minutes for good. `deploy/vm-deploy.sh` moves the checkout forward to one
commit on `main`, runs the bootstrap, and restarts the daemon only when that is owed and
safe ([#676](https://github.com/l3a0/marketlake/issues/676)). Pass the full 40-digit sha
of a commit on `main`. A pull request's page shows only 7 digits, so read the full sha of
`main`'s tip from the VM.

```bash
git -C ~/marketlake ls-remote origin refs/heads/main | cut -f1
```

```bash
sudo ~/marketlake/deploy/vm-deploy.sh --sha <sha>
```

A checkout older than the script has no `deploy/vm-deploy.sh` yet. For that first
deploy, pull inside the hours below, then deploy the commit the pull reached.

```bash
git -C ~/marketlake pull --ff-only
```

```bash
sudo ~/marketlake/deploy/vm-deploy.sh --sha "$(git -C ~/marketlake rev-parse HEAD)"
```

The terminal stays blank until the run ends, because the script prints its one line
only then. No run on the VM has been timed yet. A run that restarts the daemon takes at
least the bootstrap plus the 120 seconds it watches the daemon after the restart, and its
caps bound the longest run, a rollback included, at 200 minutes. Follow the progress from
a second shell.

```bash
sudo tail -f /var/lib/marketlake/deploy.log
```

Ctrl-C stops only the hand run's wait, which prints `outcome unknown: read deploy.log`.
The deploy keeps running as `marketlake-deploy.service`, so read its outcome in the log.

A deploy may start from 18:45 to 04:30 Eastern on a weekday night, all of Saturday,
Sunday until 16:00, and from Sunday 23:30 to Monday 04:30. Outside those hours the script
refuses with exit 3 and names the next moment one may start. The hours keep the merge and
the restart away from capture, compaction, the sweep and the Sunday job, with enough room
that a deploy which rolls back still ends before the jobs start.

It prints exactly one line, and its exit code is 0 to 3. The progress goes to
`/var/lib/marketlake/deploy.log`, which only root reads.

```bash
sudo tail -n 50 /var/lib/marketlake/deploy.log
```

`/var/lib/marketlake/deployed` holds the sha the running daemon started from. It is absent
when that is unknown, and an absent record makes the next deploy restart the daemon.

| Last line | Exit | What to do next |
| --- | --- | --- |
| `deployed: <sha>` or `deployed: <sha> (already current)` | 0 | Nothing. |
| `deployed: <sha>, with a failed step in deploy.log` | 1 | Read the log. A config step of the bootstrap failed, or the record could not be written. Fix the cause, as [Rerun the bootstrap](#rerun-the-bootstrap) says for each step, then deploy the same sha again. |
| `deployed: <sha>, but the dashboard did not restart` | 1 | Read `journalctl -u com.marketlake.dashboard`, fix the cause, then run `sudo ~/.local/state/marketlake/systemd/restart.sh dashboard`. Capture is unaffected. |
| `deployed: <sha>, with a failed step in deploy.log, but the dashboard did not restart` | 1 | The two rows above both apply. Fix the dashboard as its row says, then the failed step as its row says. |
| `rolled back to <sha>: <reason>` | 1 | The daemon runs the named sha again. Fix the reason in a pull request. A line that adds `, which has not run before` names a sha the record had not named, so watch the next session's pages. A line that ends `, with a failed step in deploy.log` means the rollback's bootstrap failed a config step or its record could not be written, so read the log and fix that as the first row says. |
| `rollback to <sha> failed: <reason>` | 1 | The daemon may be down. Check `systemctl status com.marketlake.daemon`, then recover by the hand procedure below. |
| `not restarted: the tree is at <sha>, and the last recorded deploy is <sha or none>` | 1 | The restart is still owed. The log says whether a busy job outlasted the 15-minute wait, the next refused hours were too close, or the bootstrap failed. A failed bootstrap on a checkout that moved leaves it moved only when the wait kept the rollback from running. Once that has cleared, deploy the same sha again. |
| `not deployed: the merge of <sha> failed, and the tree was restored` | 1 | The merge wrote part of the commit and failed, and the script reset the checkout to where it was. Nothing restarted. Read the log for git's error, such as a full disk, fix it, and deploy again. |
| `not deployed: <reason>` | 1, 2 or 3 | Exit 3 means not now: run again at the moment the line names, or once the named job has finished. Exit 2 is a refusal, such as a sha not on `main`, a dirty checkout or a daemon that is not running, and the line names what to fix. Exit 1 is a failure, and the log says which step. |
| `outcome unknown: read deploy.log` | 1 | The run was killed, left no result, or found the checkout in a state it could not restore. First run `systemctl is-active marketlake-deploy`, and wait while it prints `active`, because Ctrl-C on a hand run leaves the deploy running. Then compare `git -C ~/marketlake rev-parse HEAD` with `sudo cat /var/lib/marketlake/deployed`. When they differ, deploy `HEAD` again. When `git -C ~/marketlake status --porcelain` lists changes, a merge failed partway and so did its restore, so first run `git -C ~/marketlake reset --hard` and `git -C ~/marketlake clean -fd` inside the hours above. |

**The hand procedure, for a daemon that is not running.** The script refuses with exit 2
unless the daemon is `active`, because the install would start a daemon stopped on
purpose, such as during a resync or a restore. Inside the hours above, pull, rerun the
bootstrap, remove the record, and restart. After a pull, a hand restart starts a commit
the record does not name, and only the script writes the record. Removing it makes the
next deploy restart the daemon rather than trust a stale sha.

```bash
git -C ~/marketlake pull --ff-only
```

```bash
sudo ~/marketlake/deploy/vm-bootstrap.sh
```

```bash
sudo rm -f /var/lib/marketlake/deployed
```

```bash
sudo ~/.local/state/marketlake/systemd/restart.sh all
```

**Before stopping the daemon on purpose**, such as for the resync of
[#832](https://github.com/l3a0/marketlake/issues/832) or the switch back of
[#638](https://github.com/l3a0/marketlake/issues/638), start the VM if it is stopped and
remove the switch that lets it stop, as [The nightly stop](#the-nightly-stop) says, and
create it again once the work has ended. A resync runs by hand, so the stop cannot see it.
Then check that no deploy is running, and wait while this prints `active`.

```bash
systemctl is-active marketlake-deploy
```

A deploy that is running reruns the bootstrap, whose install ends with `enable --now`,
and that starts a stopped daemon again.

### Restore the lake

A restore is not part of the first boot. The shadow day captures beside the laptop and
needs no history. [#638](https://github.com/l3a0/marketlake/issues/638)'s cutover filled the
VM's lake by emptying the shadow lake with `deploy/vm-empty-shadow-lake.sh` and then
restoring from the bucket. Start the VM first if it is stopped, and remove the switch that
lets it stop, as [The nightly stop](#the-nightly-stop) says, because a restore is not a
unit the stop can see. Create the switch again once the restore and the bootstrap rerun
after it have ended. Four facts hold for any restore onto the VM.

1. The instance role reads the bucket from the first boot, so `bucket restore` runs on
   the VM through its instance profile, with no stored key.
2. A new lake volume always comes with a replacement of the instance, through
   [`replace_instance`](#replace-the-instance-and-the-approval-window). The shim is in
   the instance's `ignore_changes`, so a kept instance never learns a new volume's id,
   and its bootstrap would wait for the old device.
3. `config/vm.yaml` sets `lake_window_sessions`, and the bootstrap renders it into
   `config.yaml`, so the VM's restore rebuilds its trimmed lake
   ([#785](https://github.com/l3a0/marketlake/issues/785)). It leaves out each partition
   the bucket's `trimmed.jsonl` says the VM removed on purpose, and the rest fits the
   volume. A failed render writes nothing. On a new instance that leaves no `config.yaml`,
   and the restore exits 2 on the missing file rather than falling back to a whole-lake
   restore. On a kept instance the restore reads the file the last good render wrote, which
   sets the key too. The "When the lake is gone" steps in
   [README.md](../README.md) say how to repair a refusal over the bucket's
   `trimmed.jsonl`. A one-off whole-lake restore passes `--config` naming a copy of
   `config.yaml` with the `lake_window_sessions` line deleted, not blanked, since a blank
   value refuses, and passes `--target` naming the bucket. The VM reaches the bucket
   through its instance profile, so the copy can hold placeholder values for the four
   secrets and no secret is copied. A whole lake restored into the VM's own `lake_root`
   lasts only until the trim runs again, because the VM's own `config.yaml` still sets the
   key. Each partition it brings back beside a trim line takes the trim's recovery path and
   is trimmed again.
4. The restore refuses, with exit 2 and before any download, when the volume would be left
   with less free space than the journal reserve, 13 times the busiest sealed day in the
   bucket. Grow the lake volume first, as item 3 under
   [Rerun the bootstrap](#rerun-the-bootstrap) says: raise `lake_volume_gib`, apply, and
   rerun the bootstrap so `resize2fs` grows the filesystem. Then run the restore again. A
   whole-lake restore needs more room, not less.

The order depends on the `role` in `config/vm.yaml`.

**Under `role: primary`, the bootstrap leaves the lake empty.** A new root volume holds
no roster, and on an empty lake the roster apply refuses, so the bootstrap ends with
exit 4. The daemon then exits 2 without a roster, the capture dead-man pages during a
session, and no other job writes to the lake.

1. Let the bootstrap finish, with the roster refused.
2. Restore as the owner. The rendered `config.yaml` names the bucket, so no `--target`
   is needed.

   ```bash
   ~/marketlake/.venv/bin/python -m lake.bucket restore /srv/marketlake
   ```

3. Rerun the bootstrap, whose roster apply now passes.

**Under `role: shadow`, the lake does not stay empty.** The roster apply skips the lake
check and writes the roster. The daemon then takes the lake lock, which creates
`manifest.jsonl`, writes `journal/metadata.json` every idle minute, and writes outbox
lines between 08:25 and 18:45 ET. `bucket restore` refuses a lake holding any of that.

1. Once `systemctl is-active marketlake-deploy` prints `inactive`, stop every
   `com.marketlake.*` unit, timers included. A deploy that is running would start them
   again. The stop's own `marketlake-*` units keep running, and cannot power the VM off
   while the switch is removed, as this section's opening paragraph says.

   ```bash
   sudo systemctl stop 'com.marketlake.*'
   ```

2. Empty the lake with the tracked script, never with a typed `rm`.

   ```bash
   sudo ~/marketlake/deploy/vm-empty-shadow-lake.sh
   ```

3. Restore as the owner, with the command above.
4. Rerun the bootstrap. Its install starts the stopped units.

`deploy/vm-empty-shadow-lake.sh` is the only deliberate delete of a whole lake. The
close+15 compaction's trim deletes too, one chains partition at a time and only after the
bucket's copy is verified, recording each in `trimmed.jsonl` ([#787](https://github.com/l3a0/marketlake/issues/787)). The
resync's `--apply` deletes on purpose as well, only the covered journal segments and `bars/`
partitions that the resuming host alone recorded
([#832](https://github.com/l3a0/marketlake/issues/832)). After the
cutover the same VM runs `role: primary`, where emptying the lake would delete every
minute since the last nightly upload. So it refuses, and deletes nothing, unless all
four of these hold:

1. The lake volume is the filesystem mounted at `/srv/marketlake`, proven by its UUID.
2. The owner's `config.yaml` sets `role` to exactly `shadow`. An absent file or key
   refuses, since no `role` means primary.
3. Every loaded `com.marketlake.*` unit is `inactive` or `failed`. A waiting timer counts
   as active.
4. Nothing is mounted below `/srv/marketlake`. The delete stays out of a mount it meets
   on the way down, but it would empty a mount point sitting directly in the lake root,
   so it refuses one anywhere below it. Unmount it first.

It takes the install lock without waiting, and refuses when another run holds it. It
keeps the lock across the checks and the delete, so a concurrent bootstrap cannot start
units halfway through. It then deletes everything in the lake root except `lost+found`,
without crossing into another filesystem.

Under either role, avoid Sunday 20:00 to 23:00 ET, while the Sunday job runs
([#683](https://github.com/l3a0/marketlake/issues/683)). A replacement also drops
`chain_plan.json`, which lives on the root volume in the config directory. Capture uses
the default plan until the next close+15 re-tune writes one, and a replacement during a
session makes that night's re-tune skip the day.

### Start a stopped instance

A stopped instance captures nothing, and AWS takes back its public address. The VM stops
itself every night, as [The nightly stop](#the-nightly-stop) says, so outside the hours it
runs it is usually stopped. Before approving any apply, start a stopped instance and wait
for it to run. A VM started by hand gets an hour before the stop may power it off again.
For work that may take longer, remove the switch, as
[The nightly stop](#the-nightly-stop) says, and create it again after. A plan of a stopped
instance reads it with no public address, and only the `ignore_changes` entry for that
address keeps the plan from replacing the instance and its root volume. Starting it
first means no plan depends on that one entry.

```bash
aws ec2 start-instances --instance-ids "<instance-id>" --profile marketlake-admin --region us-east-1
```

```bash
aws ec2 wait instance-running --instance-ids "<instance-id>" --profile marketlake-admin --region us-east-1
```

An apply can also leave the instance stopped. The lake volume's attachment stops the
instance before it detaches the volume, because detaching a mounted volume from a running
instance can leave the filesystem dirty, and it never starts the instance again. A plan
that replaces the instance is unaffected, because the new instance starts on its own. A
plan that replaces only `aws_volume_attachment.lake`, such as one that changes its
`device_name`, leaves the VM stopped. After such an apply, start the instance with the
two commands above. The volume's `/etc/fstab` line mounts it at boot, and the units
start once it is mounted, provided the bootstrap's last install has run. A first boot cut
off before that install left no unit enabled, so nothing starts. Then
[rerun the bootstrap](#rerun-the-bootstrap).

### Find the VM's address

The VM has no Elastic IP, AWS's fixed address for an account, so its public address
changes at every stop and start and at every replacement. Look it up by the instance's
`marketlake:host` tag. The command prints the instance id, its state and its address,
which reads `None` while the instance is stopped. The hand-built `marketlake-measure`
carries no such tag, so it never appears here.

```bash
aws ec2 describe-instances --filters Name=tag:marketlake:host,Values=capture Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].[InstanceId,State.Name,PublicIpAddress]' --output text --profile marketlake-admin --region us-east-1
```

Since the VM stops each night, as [The nightly stop](#the-nightly-stop) says, the address
changes every morning. The roles that may call `ec2:DescribeInstances`, the deploy role
and the plan and apply roles through `ReadOnlyAccess`, can be assumed only from CI, and the
laptop's `marketlake-command` user holds no EC2 action. So each lookup and each hand start
takes an admin session. A narrow grant to a principal that is not an admin is deferred to
[#866](https://github.com/l3a0/marketlake/issues/866).

A new address goes in the `HostName` of the laptop's `~/.ssh/config` entry, which the
root README's
[Reach the dashboard on a hosted VM](../README.md#reach-the-dashboard-on-a-hosted-vm)
shows. That entry names no key, so add `IdentityFile ~/.ssh/marketlake_vm` to it. It also
sets `HostKeyAlias`, so ssh files the host key under the entry's name rather than the
address. A new address then asks nothing, and an address AWS hands out again does not warn
that its key changed. A replacement brings a new host key. When ssh refuses the entry
because a different key was seen before, clear the old key with
`ssh-keygen -R marketlake-vm`, the alias the entry names.

### The nightly stop

The VM costs about $19.6 a month running around the clock, while capture and its jobs need
it for about 62 to 68 of the week's 168 hours. So it powers itself off once the day's work
is done ([#865](https://github.com/l3a0/marketlake/issues/865)). The stop is
[#868](https://github.com/l3a0/marketlake/issues/868)'s. The start is
[#867](https://github.com/l3a0/marketlake/issues/867)'s, a schedule in AWS that starts the
VM at 07:30 ET each weekday, holidays included, and at 19:30 ET each Sunday, built in
[PR #870](https://github.com/l3a0/marketlake/pull/870). Until that schedule is applied,
nothing starts a stopped VM, which is why turning the stop on waits for it. The price is
a new way to lose the open: capture now depends on a start succeeding every weekday
morning, which the `vm-up` check below watches.

`marketlake-stop.timer` runs `deploy/vm-stop.sh` as root every 10 minutes. The script
powers the VM off only when all six of its checks pass, in this order.

1. **switch.** `/etc/marketlake/stop-when-idle` exists.
2. **uptime.** The VM has been up at least an hour, so a VM started by hand for a deploy
   or an apply gets that hour, and only that hour. Remove the switch for longer work.
3. **terminal.** `who` lists no login, and no login session is `closing`. sshd records a
   login where `who` reads it only when the login has a terminal, so an interactive SSH
   login holds the VM up and the dashboard's tunnel, `ssh -N -L`, does not. logind cannot
   tell those two apart, because systemd 255 records no terminal for an SSH session. A
   command left running after logout keeps its session `closing`. A command run over SSM,
   or as `ssh <vm> <command>`, opens no terminal either, so remove the switch first for
   work run that way.
4. **busy.** No `com.marketlake.*` job is running and no compaction runs in the daemon's
   cgroup. The deploy runs the same check, from `deploy/busy-check.sh`. It covers the
   close+15 compaction and its trim, the 18:30 sweep and the evening upload after it.
5. **window.** `python -m lake.deploy_window` allows a start now, which keeps the stop out
   of 08:00 to 18:45 ET on a weekday and 19:30 to 23:30 ET on Sunday, and out of the 210
   minutes before each.
6. **deploy.** `marketlake-deploy.service` is not running. The script then takes the deploy
   lock and the install lock without waiting, checks the deploy again, and holds both
   locks through the poweroff.

When every check passes, it pings `vm-stop` and runs `systemctl poweroff`, whatever the
ping returned. That command can return before the poweroff starts, while logind waits up
to 30 seconds for a program such as the unattended upgrade to finish. So the script keeps
both locks and waits up to 240 seconds for the poweroff, and if none comes it ends on an
error line saying so. A weekday usually stops between 18:50 and 19:00, at the first run
after the evening upload, and an upload that uses its whole budget moves that to about
20:20. A Sunday stops after 23:30.

Each run prints one line saying why it kept the VM up, or what it did. A line that starts
`vm-stop: staying up, check <n> (<name>):` is a routine refusal. A line that starts
`vm-stop: error` means a check could not tell, and the unit reads failed. Read the runs in
Eastern time.

```bash
TZ=America/New_York journalctl -u marketlake-stop
```

After a morning start, the run that powered the VM off sits in the previous boot's journal.

```bash
TZ=America/New_York journalctl -u marketlake-stop -b -1
```

**Turn the stop on** only once the start schedule is applied, or nothing starts the VM
again. The schedule is [#867](https://github.com/l3a0/marketlake/issues/867)'s, and its
section, "The start schedule", arrives with
[PR #870](https://github.com/l3a0/marketlake/pull/870). Wait until that section's
CloudTrail check has shown the schedule's `StartInstances`. Then create the switch, and
log out, since an open terminal holds the VM up.

```bash
sudo touch /etc/marketlake/stop-when-idle
```

Before logging out, run `who`. It should list only this login, since any other login
listed holds the VM up too.

**Keep the VM up** for longer work by removing the switch, and create it again after. Only
the first boot's shim writes `/etc/marketlake/`, and cloud-init runs it once, so a removed
switch stays removed across deploys and reboots. A rebuilt VM has no switch until the owner
creates one, so it stays up and `vm-stop` pages that night.

```bash
sudo rm -f /etc/marketlake/stop-when-idle
```

**Roll back** by removing the switch, then pausing `vm-stop` in healthchecks.
`sudo systemctl disable --now marketlake-stop.timer` does not last, because the next
deploy's install enables every timer again.

#### The two checks

Two healthchecks checks watch the stop and the start. Both are fed through
`python -m lake.control_plane ping`, which accepts these two slugs and no other.

| Check | Fed by | Schedule, `America/New_York` | Grace | Routing |
| --- | --- | --- | --- | --- |
| `vm-up` | `marketlake-up.timer`, at 07:40 each weekday | OnCalendar `Mon..Fri 07:35` | 15 minutes | Every integration |
| `vm-stop` | `deploy/vm-stop.sh`, just before the poweroff | OnCalendar `Mon..Fri 18:45` and `Sun 23:30`, two lines on one check | 4 hours | Email only, recommended |

healthchecks measures each deadline from the last ping, so the 07:40 ping counts for that
morning's 07:35 slot, and the night's stop counts for that evening's slot. A check that was
never pinged stays `new` and never pages. So press Ping Now on each check only once the
thing that feeds it is live: `vm-up` after the deploy that installs its timer, and
`vm-stop` on the day the switch is created. Pressed earlier, a check pages every morning or
night until then.

Every failure that only `vm-stop` catches costs about $0.02 an hour, since a hung job is
already paged by `eod-sweep`, `evening-upload` or `sunday`. So the recommendation, the
owner's to set, is to route `vm-stop` to email only, so a cost alarm does not train the
owner to ignore pages. A check created in the healthchecks UI gets every integration, so
switch ntfy off on `vm-stop` by hand. A ping healthchecks refuses, such as a 404 for a
check that was never created or is misnamed, pages through ntfy for either check, because
then the alarm itself is broken.

A shadow VM writes both pings to its outbox rather than sending them, like every other
ping. Pause both checks for the length of a shadow run, such as
[#638](https://github.com/l3a0/marketlake/issues/638)'s switch-back rehearsal, and create
the switch only once the VM is primary again.

#### When `vm-up` pages

`vm-up` pages at about 07:50 when the 07:40 ping did not arrive. `pre-open` and `capture`
then page at about 08:35, and the first captured minute is at 09:30, which leaves about
100 minutes to start the VM by hand.

1. Find the instance and its state with [Find the VM's address](#find-the-vms-address).
2. When it is `stopped`, run [Start a stopped instance](#start-a-stopped-instance). When
   the start fails on capacity, see the next section. When it is `pending`, it is already
   starting, so wait for it to read `running` and go on.
3. When it is `running` and SSH answers, run `findmnt /srv/marketlake` and `uptime -s`.
   1. When `findmnt` prints the mount and `uptime -s` shows a boot after 07:40, the start
      was late and only the page is wrong. The VM is up with its lake, and the 07:40 timer
      replays nothing on purpose, so press Ping Now on `vm-up` to clear the page.
   2. When `findmnt` prints nothing, the volume did not mount. Follow
      [When the lake volume holds an unreadable ext4](#when-the-lake-volume-holds-an-unreadable-ext4).
   3. When it prints the mount and the boot is before 07:40, the cause is the unit.
      `journalctl -u marketlake-up` shows a ping that ran and failed.
4. When it is `running` and SSH does not answer, read the console.

   ```bash
   aws ec2 get-console-output --instance-id "<instance-id>" --latest --output text --profile marketlake-admin --region us-east-1
   ```

   Each daily boot now loads any kernel the unattended upgrade installed, which used to
   wait for a deliberate reboot, so a boot that hangs is new exposure.

A start after 08:30 also pages `pre-open`, because that timer replays no missed run on
purpose.

#### When a start fails on capacity

AWS refuses a start with `InsufficientInstanceCapacity` when the zone has no `t4g.small` to
give. The zone is fixed, because the lake volume lives in `us-east-1c`, so the fast
fallback is another arm64 type in the same zone. Change the type by hand while the
instance is stopped, then start it.

```bash
aws ec2 modify-instance-attribute --instance-id "<instance-id>" --instance-type Value=t4g.medium --profile marketlake-admin --region us-east-1
```

The image is arm64, so the type must be too, such as `t4g.medium` or `m7g.medium`. The
price is drift from `instance_type` in `infra/live/`, which the next apply reverts with a
stop and a start. Approve that apply outside the hours in
[Replace the instance, and the approval window](#replace-the-instance-and-the-approval-window).

#### When `vm-stop` pages

`vm-stop` pages by about 22:45 on a weekday with no stop, and by about 03:30 Monday after a
Sunday with none. Read the evening's runs with the `journalctl -u marketlake-stop` command
above. Each refusal names its check, so act on the last one before the page.

- **switch.** The switch was removed and not created again. Create it, unless it was left
  removed on purpose.
- **terminal.** A login stayed open. Log out. `who` lists the logins. A `closing`
  session is a command left running after logout, and `loginctl list-sessions` lists the
  sessions.
- **busy.** A job ran long. Read its own journal, such as
  `journalctl -u com.marketlake.eod-sweep`. Its own check pages if it failed.
- **window.** Every run read the refused hours. That is expected before 18:45 on a weekday
  and before 23:30 on Sunday. After them, check the VM's clock with `timedatectl`.
- **deploy.** A deploy ran or held a lock. `systemctl is-active marketlake-deploy` prints
  `active` while one runs.
- **An error line.** The named check could not tell, such as a `who`, `loginctl` or
  `systemctl` that failed. Fix the cause it names. An error on check `poweroff` means
  `systemctl poweroff` failed, or returned and no poweroff came within 240 seconds.

### The duplicate-name check

The code creates a key pair and a security group, both named `marketlake-vm`. A key
pair's name is unique in a region, and a security group's in a VPC, so one made in the
console under the same name fails the apply at that resource. The owner made the
measurement VM's key pair and security group in the console, with names recorded nowhere,
so check before the first apply. Each command should print nothing.

```bash
aws ec2 describe-key-pairs --filters Name=key-name,Values=marketlake-vm --query 'KeyPairs[].KeyName' --output text --profile marketlake-admin --region us-east-1
```

```bash
aws ec2 describe-security-groups --filters Name=group-name,Values=marketlake-vm --query 'SecurityGroups[].GroupId' --output text --profile marketlake-admin --region us-east-1
```

When either prints a name or an id, delete the console-made one before the apply. A
key pair can go at once, even while an instance launched with it runs. EC2 copied the
public key into that instance's `authorized_keys` at launch, so deleting the key pair
record leaves SSH to it working. A security group must wait until no instance is attached
to it, because EC2 refuses to delete one that is in use. On 2026-10-07 the key pair check
printed `marketlake-vm`, an ed25519 key made on 2026-10-05 for the measurement VM, with
the same fingerprint as `~/.ssh/marketlake_vm.pub`. The security group check printed
nothing.

### Replace the instance, and the approval window

**Never approve an `infra` apply between 09:25 and 16:15 ET on a session day.** There is
no exemption for a plan that touches neither the instance nor the lake volume, because
[#664](https://github.com/l3a0/marketlake/issues/664)'s pass 9 found three edits that
stop capture without touching either.

1. A security group edit that leaves no egress rule, which cuts the daemon off from the
   Schwab API.
2. Adopting the default VPC's route table.
3. An AMI made from the instance, which reboots it by default.

Changes to the instance itself stop capture too. A changed `instance_type` stops and
starts the instance, and a replacement of the instance or of the volume's attachment
stops it.

On a weekday evening an apply that stops the instance or cuts its egress also kills the
upload after the vendor sweep, which runs in the eod-sweep unit from about 18:32 ET
([#833](https://github.com/l3a0/marketlake/issues/833)). That loses nothing, since the
bucket's manifest goes up last and the next upload re-sends what is pending, but it
pages "Evening upload". An apply that turns the instance role's write half off fails
the upload's PUTs and pages the same way. Approve once
`systemctl is-active com.marketlake.eod-sweep.service` on the VM prints `inactive`.

A new instance comes only from a manual run with the `replace_instance` input set. It
adds `-replace=aws_instance.vm` to the apply's plan and names no other address, so the
lake volume and its data stay. The laptop apply could replace the instance too, but it
skips the approval, so the dispatch is the one reviewed route.

```bash
gh workflow run infra.yml --repo l3a0/marketlake --ref main -f replace_instance=true
```

Three rules go with it.

1. **Dispatch while no merge that starts an Infra run is pending, and merge none while
   the run waits.** A merge starts such a run when it changes `infra.yml`, or anything
   under `infra/` other than Markdown. One that lands while the dispatch waits for
   approval drops it in one of two ways. The dispatch can go stale and skip, with a line
   saying a newer commit's run applies the change, which is false for a replacement. Or
   the merge's run queues in the `infra-apply` group and cancels the waiting dispatch.
   Either way, the merge's run applies without `-replace`.
2. **Approve it outside the window above.**
3. **Confirm the replacement in the run's summary.** The approval comes before the job
   plans, so the summary, "Plan this run applies to infra/live", appears only as the run
   applies. Read it once the run ends. It is expected to list `aws_instance.vm` as
   `replace`, with `aws_volume_attachment.lake` replaced beside it. No run has shown this
   summary yet, because no dispatch has run against a VM that exists. When
   it lists no replacement,
   the instance was not replaced, so dispatch again.

The new instance boots through the same shim. Its bootstrap finds the existing ext4
filesystem, prints that the volume mounts without formatting, and mounts the same lake.
Its address is new, as [Find the VM's address](#find-the-vms-address) says.

The first replacement is a live check, run on the first apply's own evening right after
the first boot is read, while the volume holds only that boot's files. List
`/srv/marketlake/journal/` before the dispatch. Afterwards, the new instance's
`/var/log/cloud-init-output.log` should say the volume mounts without formatting, and
the same files should still be there.

### Add the VM's two lines to `live.tfvars`

Only a laptop plan or apply of `infra/live/`, as in
[Apply `infra/live/` from the laptop](#apply-infralive-from-the-laptop), needs these
lines. CI reads the same two values from `OWNER_SSH_CIDR` and `SSH_PUBLIC_KEY`, so they
are no step before the first apply. Without them a laptop plan asks for both. The owner
added both lines on 2026-10-07, so this applies only to a new laptop or a changed
address. Each command appends one line to the file step 4 wrote, and reads its value from
the network or the key file, so the address never appears on screen.

```bash
printf 'owner_ssh_cidr = "%s/32"\n' "$(curl -fsS https://checkip.amazonaws.com)" >> ~/.config/marketlake/infra/live.tfvars
```

```bash
printf 'ssh_public_key = "%s"\n' "$(cat ~/.ssh/marketlake_vm.pub)" >> ~/.config/marketlake/infra/live.tfvars
```

Run each once. A second run adds a second line for the same name, which OpenTofu
refuses. After the home address changes, edit the `owner_ssh_cidr` line in place.

## Deploy from CI

Since the cutover the VM is the primary capture host, so code merged to `main` has to
reach it, and a deploy at the wrong moment loses captured minutes for good.
`.github/workflows/deploy.yml` asks the VM to deploy each push to `main`, once the owner
approves the run in
[the `deploy` environment](https://github.com/l3a0/marketlake/deployments/activity_log?environments_filter=deploy)
([#676](https://github.com/l3a0/marketlake/issues/676)). Nobody logs in and no key is
stored. The design's "Infrastructure, defined" carries the reasoning.

The run takes four steps.

1. It refuses an empty `AWS_DEPLOY_ROLE_ARN`.
2. It skips, with a green summary, when `main` has moved past the run's commit. The newer
   commit's own run deploys it.
3. It assumes `marketlake-deploy` through OIDC.
4. It runs `deploy/send-deploy.sh`. The script finds the one instance tagged
   `marketlake:host = capture` in the states
   [Find the VM's address](#find-the-vms-address) lists. When that instance is running,
   the script sends it the SSM document `marketlake-deploy` with the commit and a
   ten-minute delivery window, waits for the command to end, and prints one summary.

The VM stops itself once each day's work is done, as [The nightly stop](#the-nightly-stop)
says, so a deploy approved in the evening or at the weekend can find it stopped. The run
then sends nothing and ends on a line that says so. Find the instance's id with
[Find the VM's address](#find-the-vms-address), start it with
[Start a stopped instance](#start-a-stopped-instance), and re-run the job once it runs.
The started VM gets an hour before the stop considers it, and a deploy that starts inside
that hour holds it up until the deploy ends. For longer work, remove the switch as that
section says.

The VM, not the moment of approval, decides whether a deploy is safe. An approval during
the hours it refuses fails the run with exit 3 and names the time a deploy may start. A
re-run after that time asks for approval again, and then deploys.

The repository is public, so the run's log and its summary show only the summary line,
`StatusDetails` and `ResponseCode`. The VM keeps the full output in
`/var/lib/marketlake/deploy.log`. This table says what each summary means.

| Summary | Exit | What it means |
| --- | --- | --- |
| A line from the table of last lines that `vm-deploy.sh` prints, under [Deploy a commit](#deploy-a-commit) | 0 to 3 | The VM ran the deploy and printed that line. |
| `vm-deploy.sh is missing on the VM, so run the manual first deploy` | 1 | The VM's checkout predates `vm-deploy.sh`. Run step 1 of [The owner's first deploy](#the-owners-first-deploy). |
| `not delivered, so re-run` | 1 | The command never reached the VM, for example while the SSM agent was down. Re-run the job. |
| `not started` | 1 | The command was cancelled before it started. |
| `outcome unknown: ...` | 1 | Compare `HEAD` with `/var/lib/marketlake/deployed` on the VM, and read `deploy.log`. When they differ, re-run the deploy for `HEAD`. |
| `not sent: the VM is stopped. ...` | 1 | The VM stopped itself for the night. Start it as the line says, then re-run the job once it runs. |
| `not sent: the VM is still starting. ...` | 1 | Wait for it to run, then re-run the job. |
| `not sent: ...` | 1 | Nothing reached SSM. The line names the cause: a `GITHUB_SHA` that is not a 40-digit commit, an empty `DEPLOY_DOCUMENT`, `DEPLOY_TAG_KEY` or `DEPLOY_TAG_VALUE`, the error code of a failed `DescribeInstances` or `SendCommand`, the count of instances with the tag when it is not exactly one, or a `DescribeInstances` reply that is not an instance id and its state. |

### The `deploy` environment and its secret

[The `deploy` environment](https://github.com/l3a0/marketlake/deployments/activity_log?environments_filter=deploy)
requires the owner's approval, as
[the `infra` environment](https://github.com/l3a0/marketlake/deployments/activity_log?environments_filter=infra)
does, until the job has run cleanly enough times to drop it.
[#851](https://github.com/l3a0/marketlake/issues/851) holds that decision. Only the owner
creates it, edits it or approves its runs, per the "Deployment approvals" section of
[`CLAUDE.md`](../CLAUDE.md#deployment-approvals-owner-directive-2026-10-06). GitHub
creates a missing environment, with no protection rules, the first time a job names it.
So the owner creates `deploy`, or protects the one a run created, with the same settings
[step 8](#8-create-the-infra-environment) gives `infra`. The `PUT` below does either.

```bash
gh api -X PUT repos/l3a0/marketlake/environments/deploy --input - <<'EOF'
{"prevent_self_review": false,
 "can_admins_bypass": false,
 "reviewers": [{"type": "User", "id": <github-user-id>}],
 "deployment_branch_policy": {"protected_branches": false, "custom_branch_policies": true}}
EOF
```

```bash
gh api -X POST repos/l3a0/marketlake/environments/deploy/deployment-branch-policies -f name=main -f type=branch
```

Read it back as step 8 does, with `deploy` in place of `infra`. The protection must list
`required_reviewers` and `branch_policy`, the branch policies must print `["main"]`, and
`can_admins_bypass` must print `false`.

The deploy role's ARN is a secret on the `deploy` environment, never on the repository.
So a run in an environment nobody protected finds it empty and stops before it assumes
anything. Set it once the bootstrap apply has created the role.

```bash
aws iam get-role --role-name marketlake-deploy --query Role.Arn --output text --profile marketlake-admin | gh secret set AWS_DEPLOY_ROLE_ARN --env deploy --repo l3a0/marketlake
```

```bash
gh secret list --env deploy --repo l3a0/marketlake
```

### The owner's first deploy

A deploy may start, and so may a pull by hand, from 18:45 to 04:30 ET on a weekday
night, all of Saturday, Sunday until 16:00, and from Sunday 23:30 to Monday 04:30. The
first deploy takes seven steps, in order.

1. Pull on the VM inside those hours and run the manual first deploy that
   [Deploy a commit](#deploy-a-commit) describes, so `vm-deploy.sh` exists there.
2. Create the `deploy` environment with the owner as required reviewer, `main` as its
   only deployment branch, and admin bypass off, as
   [The `deploy` environment and its secret](#the-deploy-environment-and-its-secret)
   says. Leave self-review allowed, since the owner merges.
3. Apply `infra/bootstrap/` from `main`, per
   [Changing the bootstrap](#changing-the-bootstrap). Then set `AWS_DEPLOY_ROLE_ARN` as
   a secret on `deploy`.
4. Approve the waiting run in
   [the `infra` environment](https://github.com/l3a0/marketlake/deployments/activity_log?environments_filter=infra)
   that the merge of [PR #854](https://github.com/l3a0/marketlake/pull/854) queued, which
   creates the document, outside 09:25 to 16:15 ET.
5. Approve the run that the merge queued in
   [the `deploy` environment](https://github.com/l3a0/marketlake/deployments/activity_log?environments_filter=deploy),
   inside the hours above, and read its
   summary.
6. Check what only the VM can show.
   1. Confirm that this command prints `KillMode=process`.

      ```bash
      systemctl show -p KillMode snap.amazon-ssm-agent.amazon-ssm-agent.service
      ```

   2. Confirm that this command prints the format the busy check parses.

      ```bash
      systemctl list-units --type=service --all --no-legend --plain 'com.marketlake.*'
      ```

   3. Start a hand run, and from a second shell send SIGKILL to the wrapper's process
      group, as SSM's timeout would. Then confirm that `marketlake-deploy.service` runs
      to the end, that `/var/lib/marketlake/deployed` names the sha, that
      `journalctl -u marketlake-deploy` holds no deploy output, and that `deploy.log`
      shows the run.
7. Check what IAM allows, from an admin session. Read the role's ARN, the VM's instance id
   and the account into shell variables, so no command prints them.

   ```bash
   role="$(aws iam get-role --role-name marketlake-deploy --query Role.Arn --output text --profile marketlake-admin)"
   ```

   ```bash
   instance="$(aws ec2 describe-instances --filters Name=tag:marketlake:host,Values=capture Name=instance-state-name,Values=running --query 'Reservations[].Instances[].InstanceId' --output text --profile marketlake-admin --region us-east-1)"
   ```

   ```bash
   account="$(aws sts get-caller-identity --query Account --output text --profile marketlake-admin)"
   ```

   `ssm:SendCommand` with `AWS-RunShellScript` must print `implicitDeny`.

   ```bash
   aws iam simulate-principal-policy --policy-source-arn "$role" --action-names ssm:SendCommand --resource-arns "arn:aws:ssm:us-east-1::document/AWS-RunShellScript" --query 'EvaluationResults[].EvalDecision' --output text --profile marketlake-admin
   ```

   With `marketlake-deploy`, a send to the VM must be allowed only with the tag in the
   context. The first command must print `allowed` and the second `implicitDeny`.

   ```bash
   aws iam simulate-principal-policy --policy-source-arn "$role" --action-names ssm:SendCommand --resource-arns "arn:aws:ec2:us-east-1:$account:instance/$instance" --context-entries "ContextKeyName=ssm:resourceTag/marketlake:host,ContextKeyValues=capture,ContextKeyType=string" --query 'EvaluationResults[].EvalDecision' --output text --profile marketlake-admin
   ```

   ```bash
   aws iam simulate-principal-policy --policy-source-arn "$role" --action-names ssm:SendCommand --resource-arns "arn:aws:ec2:us-east-1:$account:instance/$instance" --query 'EvaluationResults[].EvalDecision' --output text --profile marketlake-admin
   ```

   The same send on the document must print `allowed`.

   ```bash
   aws iam simulate-principal-policy --policy-source-arn "$role" --action-names ssm:SendCommand --resource-arns "arn:aws:ssm:us-east-1:$account:document/marketlake-deploy" --query 'EvaluationResults[].EvalDecision' --output text --profile marketlake-admin
   ```

   The trust's `ref` condition is proven only by the live run in step 5, as it is for
   the apply role.

## Bootstrap changes already known

No open issue on the MVP 2 path changes `infra/bootstrap/`.
[#704](https://github.com/l3a0/marketlake/issues/704) is deferred. If it is taken up, it
changes `infra/bootstrap/`, where the apply role's trust in `roles.tf` grows to accept a
second environment, and its body gives the order for that change.
