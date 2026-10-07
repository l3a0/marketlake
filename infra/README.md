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
   OIDC provider, and two roles that GitHub Actions assumes without a stored AWS key. The
   OIDC provider lets a workflow trade a short-lived token that GitHub signs for temporary
   AWS credentials. The plan role reads, and pull requests may assume it. The apply role
   writes, and only the `infra` environment on `main` may assume it. CI cannot apply the
   configuration that creates it, so the owner applies this one from the laptop.
2. `infra/live/` holds the backup bucket, the instance role `marketlake-instance` with its
   read of the bucket and the config parameters, the laptop's one IAM user,
   `marketlake-command` ([#737](https://github.com/l3a0/marketlake/issues/737)), and the
   hosted VM with its security group, key pair and lake volume. That user can only assume
   two roles: `marketlake-backup`, which reaches the bucket, and
   `marketlake-token-writer`, which writes the Schwab token's parameter.
   `.github/workflows/infra.yml` plans it on each pull request from a branch here, and
   applies it after a merge to `main` once the owner approves the run. Either runs only
   when the change reaches the workflow or a file under `infra/` other than Markdown,
   since no configuration reads Markdown. A manual run can also replace the VM, as
   [The hosted VM](#the-hosted-vm) says.

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

`<policy-name>` is the name of the old `marketlake-backup` user's inline policy. It lives
in a repository variable, which GitHub does not mask in logs, so it is not a secret. It is
still written only on the laptop and in GitHub's settings. Since
[#737](https://github.com/l3a0/marketlake/issues/737) no resource uses it, so any valid
IAM policy name works until [#741](https://github.com/l3a0/marketlake/issues/741) removes
the variable.

The rest are read when they are needed.

- `<github-user-id>` is the owner's numeric GitHub id, which step 8 reads.
- `<backup-key-id>` is the id of the old `marketlake-backup` user's access key, which
  [Retire the old users](#retire-the-old-users) reads.
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
account, differs in five steps.

1. Step 3 finds no `marketlake-backup` user, so it skips the user lookups.
2. Steps 4 and 9 take any valid IAM policy name for `<policy-name>`, until
   [#741](https://github.com/l3a0/marketlake/issues/741) removes the variable.
3. Step 7 applies the bootstrap from a worktree at `origin/main` rather than at a pull
   request's branch.
4. Step 10 drops out, since no pull request needs a plan.
5. Step 11 replaces the merge with a manual run of `infra.yml` on `main`, which is
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

Nothing tracked names the backup bucket, the old backup user's policy, or whether the
account already holds a GitHub OIDC provider, so read each from AWS.

List the buckets, to find `<backup-bucket>`.

```bash
aws s3api list-buckets --profile marketlake-admin
```

The next two commands read the old `marketlake-backup` user, which the recorded run
imported. [#737](https://github.com/l3a0/marketlake/issues/737) replaced that user with a
role of the same name and stopped importing it. Once the owner has deleted the user, or in
a fresh account, there is no user to read, so skip both, and use any valid IAM policy name
for `<policy-name>` until [#741](https://github.com/l3a0/marketlake/issues/741).

List the IAM users, to confirm `marketlake-backup` exists.

```bash
aws iam list-users --profile marketlake-admin
```

List the user's inline policies. The one name it prints is `<policy-name>`. An import
adopts a resource that already exists into the state, rather than creating it. The
recorded run's import of the user's policy needed that name, so there was no safe guess.

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

`live.tfvars` holds the two values the live configuration needs. Nothing reads
`backup_policy_name` since [#737](https://github.com/l3a0/marketlake/issues/737), yet the
variable still has no default, so it takes any valid IAM policy name until
[#741](https://github.com/l3a0/marketlake/issues/741) removes it.

```bash
cat > ~/.config/marketlake/infra/live.tfvars <<'EOF'
backup_bucket      = "<backup-bucket>"
backup_policy_name = "<policy-name>"
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

On the first run the plan imports the state bucket and creates eight resources.

1. The state bucket's versioning.
2. The GitHub OIDC provider.
3. The plan role.
4. The plan role's attachment of AWS's `ReadOnlyAccess`.
5. The plan role's inline policy.
6. The apply role.
7. The apply role's attachment of `ReadOnlyAccess`.
8. The apply role's inline policy.

When step 3 found an existing provider, the plan imports the provider instead of creating
it, so it shows two imports and seven creates. The plan destroys nothing. A later run,
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

### 9. Set the secrets and the variables

Nothing tracked names the account, the buckets, the user's policy, the owner's address
or the VM's key, so the workflow reads them from seven settings.

1. The secret `AWS_APPLY_ROLE_ARN`, the apply role's ARN, the identifier AWS gives each
   resource. It sits on the `infra` environment, so the apply job can read it only after
   the approval.
2. The secret `AWS_PLAN_ROLE_ARN`, the plan role's ARN, on the repository.
3. The secret `TF_STATE_BUCKET`, the state bucket's name, on the repository.
4. The secret `BACKUP_BUCKET`, the backup bucket's name, on the repository.
5. The variable `BACKUP_POLICY_NAME`, the name of the old `marketlake-backup` user's
   inline policy, on the repository. CI still refuses an empty value, and nothing reads it
   since [#737](https://github.com/l3a0/marketlake/issues/737), so any valid IAM policy
   name works until [#741](https://github.com/l3a0/marketlake/issues/741) removes it.
6. The secret `OWNER_SSH_CIDR`, the owner's address as a `/32`, the one address that may
   SSH to the VM, on the repository.
7. The variable `SSH_PUBLIC_KEY`, the public half of the VM's SSH key, on the
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

```bash
gh variable set BACKUP_POLICY_NAME --body "<policy-name>" --repo l3a0/marketlake
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

The repository's variables should include `BACKUP_POLICY_NAME` and `SSH_PUBLIC_KEY`.

```bash
gh variable list --repo l3a0/marketlake
```

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
[Start a stopped instance](#start-a-stopped-instance), before running any plan. Never
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
step 7, then approve its live apply. Apply it only from the bootstrap pull
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
   user, the two roles and their policies. Its plan summary also shows four `forget` rows,
   one for each old user and its policy, which leave the state and keep existing in AWS.
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
      until [#676](https://github.com/l3a0/marketlake/issues/676)'s deploy. So the apply
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
      only as [Rerun the bootstrap](#rerun-the-bootstrap) describes.

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
`s3:PutObject` alone, stays off until
[#638](https://github.com/l3a0/marketlake/issues/638)'s cutover turns it on.

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
   times on any error, and runs `deploy/linux-install.sh`, which starts the units. Then
   it renders `config.yaml` from `config/vm.yaml`, the parameters and the tag, pulls the
   token, and applies the roster. It takes the install lock around each attempt of
   these steps and releases it during the 20 seconds it waits before a retry. One
   attempt against an SSM endpoint that hangs holds the lock about 160 seconds, four
   botocore attempts of up to 40 seconds each, well under the 600 seconds another
   install waits for it. A metadata read cannot hang that long, because each request
   to the metadata service times out after a second.

Each line the bootstrap prints starts `vm-bootstrap:`, and a run that finished prints
`vm-bootstrap: done` last. A failed first boot shows in `/var/log/cloud-init-output.log`
and the journal, not in healthchecks. The VM runs as `role: shadow`, and a shadow's pings
go to its own `journal/outbox/` rather than to healthchecks.

Read the first boot on the apply's evening. Find the address as in
[Find the VM's address](#find-the-vms-address), then open a session from the laptop.

```bash
ssh -i ~/.ssh/marketlake_vm ubuntu@"<vm-address>"
```

On the VM, cloud-init's summary says `status: done` for a boot whose script exited 0.

```bash
cloud-init status --long
```

The bootstrap's own lines sit at the end of cloud-init's log.

```bash
tail -n 40 /var/log/cloud-init-output.log
```

The daemon prints its role at every start, so its journal should hold
`daemon: role=shadow`.

```bash
TZ=America/New_York journalctl -u com.marketlake.daemon | grep 'role='
```

The daemon writes `journal/metadata.json` every idle minute, so its time should move
between two runs a minute apart.

```bash
stat -c %y /srv/marketlake/journal/metadata.json
```

The root volume is 16 GiB, and the checkout, the venv and `uv`'s cache are estimated at 5
to 6 GB of it.

```bash
df -h /
```

```bash
du -sh ~/.cache/uv ~/marketlake/.venv
```

At the next open, data segments appear under `/srv/marketlake`, and the outbox under
`/srv/marketlake/journal/outbox/` gains ping lines. The build plan lists both reads as a
live check, beside the replacement in
[Replace the instance](#replace-the-instance-and-the-approval-window), which runs on the
same evening as the first boot.

### Rerun the bootstrap

`deploy/vm-bootstrap.sh` can run again at any time, as root, over SSH. Each step skips
work that is already done, so a second run under the same conditions changes nothing.

```bash
sudo ~/marketlake/deploy/vm-bootstrap.sh
```

Each failure prints one line naming the step, and the exit code says how the run ended.

1. Exit 0 means every step passed.
2. Exit 2 means the bootstrap itself refused, which happens only before the install, such
   as on a bad `bootstrap.conf` or an `/etc/fstab` with no entry for `/`. It also refuses
   three states of the lake volume, and formats nothing in any of them.
   1. The volume holds something other than ext4.
   2. A read of 4 KiB block 0 or 32768 failed or came back short.
   3. `blkid` finds no filesystem, but an ext4 superblock sits in block 0 or 32768. The
      line prints a read-only `e2fsck -n` command, and
      [When the lake volume holds an unreadable ext4](#when-the-lake-volume-holds-an-unreadable-ext4)
      says what to do.
3. Exit 1 covers every other failure. A disk step or the install that fails stops the
   run at once, and the install's own refusal counts as a failure here.
   A failed render, token pull or roster apply skips only the steps that need it, and
   the run then ends with exit 1, even when the step itself refused with its own exit 2.

So read every line, not only the last.

Three things call for a rerun.

1. **A parameter or the tag was missing at first boot.** A missing parameter makes
   the render refuse, and a missing tag makes it exit 3 on all six attempts. Either
   way the bootstrap ended with exit 1, and the daemon restarts every ten seconds
   without a `config.yaml`. Put a missing parameter, or fix a missing tag in
   `infra/live/vm.tf` through an approved apply, as the render's line says. Then rerun.
2. **Code merged after the apply.** Nothing pulls on its own until
   [#676](https://github.com/l3a0/marketlake/issues/676). After the close, pull as the
   owner, rerun, and restart, because the install restarts nothing. Timers start fresh
   processes and pick up new code by themselves.

   ```bash
   git -C ~/marketlake pull --ff-only
   ```

   ```bash
   sudo ~/marketlake/deploy/vm-bootstrap.sh
   ```

   ```bash
   sudo ~/.local/state/marketlake/systemd/restart.sh all
   ```

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

### Restore the lake

A restore is not part of the first boot. The shadow day captures beside the laptop and
needs no history, and how the cutover fills the VM's lake is
[#638](https://github.com/l3a0/marketlake/issues/638)'s question. Two facts hold for any
restore onto the VM.

1. The instance role reads the bucket from the first boot, so `bucket restore` runs on
   the VM through its instance profile, with no stored key.
2. A new lake volume always comes with a replacement of the instance, through
   [`replace_instance`](#replace-the-instance-and-the-approval-window). The shim is in
   the instance's `ignore_changes`, so a kept instance never learns a new volume's id,
   and its bootstrap would wait for the old device.

The order depends on the `role` in `config/vm.yaml`.

**Under `role: primary`, the bootstrap leaves the lake empty.** A new root volume holds
no roster, and on an empty lake the roster apply refuses, so the bootstrap ends with
exit 1. The daemon then exits 2 without a roster, the capture dead-man pages during a
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

1. Stop every unit, timers included.

   ```bash
   sudo systemctl stop 'com.marketlake.*'
   ```

2. Empty the lake with the tracked script, never with a typed `rm`.

   ```bash
   sudo ~/marketlake/deploy/vm-empty-shadow-lake.sh
   ```

3. Restore as the owner, with the command above.
4. Rerun the bootstrap. Its install starts the stopped units.

`deploy/vm-empty-shadow-lake.sh` is the only deliberate delete of lake data. After the
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

A stopped instance captures nothing, and AWS takes back its public address. Before
approving any apply, start a stopped instance and wait for it to run. A plan of a stopped
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
start once it is mounted.

### Find the VM's address

The VM has no Elastic IP, AWS's fixed address for an account, so its public address
changes at every stop and start and at every replacement. Look it up by the instance's
`marketlake:host` tag. The command prints the instance id, its state and its address,
which reads `None` while the instance is stopped. The hand-built `marketlake-measure`
carries no such tag, so it never appears here.

```bash
aws ec2 describe-instances --filters Name=tag:marketlake:host,Values=capture Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].[InstanceId,State.Name,PublicIpAddress]' --output text --profile marketlake-admin --region us-east-1
```

A new address goes in the `HostName` of the laptop's `~/.ssh/config` entry, which the
root README's
[Reach the dashboard on a hosted VM](../README.md#reach-the-dashboard-on-a-hosted-vm)
shows. That entry names no key, so add `IdentityFile ~/.ssh/marketlake_vm` to it. A
replacement also brings a new host key. When ssh refuses an address because a different
key was seen there before, clear the old key with `ssh-keygen -R "<vm-address>"`.

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

## Bootstrap changes already known

One open issue changes `infra/bootstrap/`.
[#676](https://github.com/l3a0/marketlake/issues/676) adds a deploy role, and follows the
order under [Changing the bootstrap](#changing-the-bootstrap). The issue carries its own
scope.
