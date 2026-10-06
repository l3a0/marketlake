# Infrastructure runbook

The hosted deployment gets rebuilt at a cutover, after a VM dies, at a resize, and in a
disaster restore. With its AWS resources in code, each rebuild is one reviewed apply
instead of a walk through the console, and this runbook is the part that code cannot do
for itself. The design's
[Deployment section](../docs/design.md#deployment-laptop-now-dedicated-later) carries the
reasoning, under "Infrastructure, defined", and
[#664](https://github.com/l3a0/marketlake/issues/664) carries the plan.

The code is two OpenTofu configurations. OpenTofu keeps a record of which real resources
each one manages, called its state, and both states sit in one S3 bucket under separate
keys.

1. `infra/bootstrap/` holds what CI needs before it can run: the state bucket, GitHub's
   OIDC provider, and two roles that GitHub Actions assumes without a stored AWS key. The
   OIDC provider lets a workflow trade a short-lived token that GitHub signs for temporary
   AWS credentials. The plan role reads, and pull requests may assume it. The apply role
   writes, and only the `infra` environment on `main` may assume it. CI cannot apply the
   configuration that creates it, so the owner applies this one from the laptop.
2. `infra/live/` holds the backup bucket, its IAM user `marketlake-backup`, and the
   instance role `marketlake-instance`. `.github/workflows/infra.yml` plans it on each
   pull request from a branch here, and applies it after a merge to `main` once the owner
   approves the run.

The laptop also applies any change the apply role may not make, such as the backup
bucket's versioning or a role's trust policy.

This repository is public. Every value below appears as a placeholder, and the real
values stay out of every tracked file, every issue and every comment.

- `<state-bucket>` is the state bucket's name.
- `<backup-bucket>` is the backup bucket's name.
- `<policy-name>` is the name of `marketlake-backup`'s inline policy.
- `<github-user-id>` is the owner's numeric GitHub id.
- `<branch>` and `<n>` are the pull request's branch and number.
- `<run-id>` and `<lock id>` are ids that an earlier command prints, named where each one
  appears.

## The first run, in order

### 1. Install the tools

```bash
brew install awscli opentofu
```

`aws login` needs AWS CLI 2.32.0 or later, so check the version.

```bash
aws --version
```

OpenTofu must be 1.13 or a later 1.x release, which both configurations require. CI pins
its own copy in `infra.yml`. CloudShell does not work for any of this, because its 1 GB
home cannot hold the AWS provider.

### 2. Open an admin session under a named profile

```bash
aws login --profile marketlake-admin
```

The first `aws login` prompts for a region. Answer `us-east-1`.

Never run a plain `aws login`. It writes the `default` profile, and then every process on
the laptop that reads the default credential chain, agent sessions included, acts as
account admin. Check that the session works.

```bash
aws sts get-caller-identity --profile marketlake-admin
```

It prints the account id. Never paste that id into an issue or a comment. The default
chain should still hold no credentials, so the same command without the profile fails.

```bash
aws sts get-caller-identity
```

A command run through Claude Code's `!` prefix may start a fresh shell, so an `export`
does not last from one command to the next. That is why every `aws` command below passes
`--profile marketlake-admin` and every `tofu` command starts with
`AWS_PROFILE=marketlake-admin`.

### 3. Read the values recorded nowhere

Nothing tracked names the backup bucket, the user's policy, or whether the account
already holds a GitHub OIDC provider, so read each from AWS.

List the buckets, to find `<backup-bucket>`.

```bash
aws s3api list-buckets --profile marketlake-admin
```

List the IAM users, to confirm `marketlake-backup` exists.

```bash
aws iam list-users --profile marketlake-admin
```

List the user's inline policies. The one name it prints is `<policy-name>`. The import
needs it, so there is no safe guess.

```bash
aws iam list-user-policies --user-name marketlake-backup --profile marketlake-admin
```

Read the backup bucket's lifecycle rules. The output may open a pager, and `q` exits it.

```bash
aws s3api get-bucket-lifecycle-configuration --bucket <backup-bucket> --profile marketlake-admin
```

The code in `infra/live/bucket.tf` expects four rules, with the ids `manifest`,
`quarantine`, `actions` and `journal`. Each one has a `Filter.Prefix` under `lake/` and
expires noncurrent versions after 30 days. A different id or shape shows up in the first
plan as an in-place update. On 2026-10-06 the four rules matched the code.

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
every checkout, because a fresh worktree has no ignored files.

```bash
mkdir -p ~/.config/marketlake/infra
```

`bootstrap.tfbackend` names the state bucket for `infra/bootstrap/`. OpenTofu calls the
place it keeps state the backend. `<state-bucket>` is a new name, and S3 bucket names are
unique across all of AWS.

```bash
printf 'bucket = "%s"\n' '<state-bucket>' > ~/.config/marketlake/infra/bootstrap.tfbackend
```

`bootstrap.tfvars` holds the bootstrap's three inputs.

```bash
cat > ~/.config/marketlake/infra/bootstrap.tfvars <<'EOF'
state_bucket               = "<state-bucket>"
backup_bucket              = "<backup-bucket>"
adopt_github_oidc_provider = false
EOF
```

`live.tfbackend` names the same bucket for `infra/live/`.

```bash
printf 'bucket = "%s"\n' '<state-bucket>' > ~/.config/marketlake/infra/live.tfbackend
```

`live.tfvars` holds the live configuration's two inputs.

```bash
cat > ~/.config/marketlake/infra/live.tfvars <<'EOF'
backup_bucket      = "<backup-bucket>"
backup_policy_name = "<policy-name>"
EOF
```

### 5. Create the state bucket

```bash
aws s3api create-bucket --bucket <state-bucket> --region us-east-1 --profile marketlake-admin
```

In `us-east-1` the command takes no `--create-bucket-configuration`. A new bucket comes
encrypted and with public access blocked, but not versioned. The bootstrap adopts it and
turns versioning on.

### 6. Apply the bootstrap from a separate worktree

Apply the bootstrap from the pull request's branch at its final head, in a worktree of
its own. A worktree is a second checkout of the repository, which git keeps beside the
first. Never switch the main checkout to the branch, because the main checkout is the
code the live daemon runs. Run this from the main checkout.

```bash
git fetch origin
```

```bash
git worktree add --detach ~/marketlake-infra-<n> origin/<branch>
```

Run the rest of this step from the worktree's root.

```bash
cd ~/marketlake-infra-<n>
```

Check that the worktree sits at the pull request's final head. The two commands should
name the same commit.

```bash
git log --oneline -1
```

```bash
gh pr view <n> --repo l3a0/marketlake --json headRefOid --jq .headRefOid
```

Initialise the configuration against the state bucket.

```bash
AWS_PROFILE=marketlake-admin tofu -chdir=infra/bootstrap init -backend-config="$HOME/.config/marketlake/infra/bootstrap.tfbackend"
```

Plan it, and read the plan before applying anything.

```bash
AWS_PROFILE=marketlake-admin tofu -chdir=infra/bootstrap plan -var-file="$HOME/.config/marketlake/infra/bootstrap.tfvars"
```

An import adopts a resource that already exists into the state, rather than creating it.
On the first run the plan imports the state bucket and creates eight resources.

1. The state bucket's versioning.
2. The GitHub OIDC provider.
3. The plan role.
4. The plan role's attachment of AWS's `ReadOnlyAccess`.
5. The plan role's inline policy.
6. The apply role.
7. The apply role's attachment of `ReadOnlyAccess`.
8. The apply role's inline policy.

When step 3 found an existing provider, the provider moves from the creates to the
imports. The plan destroys nothing. A later run, against state that already exists, plans
only what the pull request changes. Treat a plan that destroys anything as coming from a
stale checkout until shown otherwise.

```bash
AWS_PROFILE=marketlake-admin tofu -chdir=infra/bootstrap apply -var-file="$HOME/.config/marketlake/infra/bootstrap.tfvars"
```

The apply plans again and prints its own summary line. Type `yes` only if that line
matches the plan just read. On 2026-10-06 the first apply reported 1 imported, 8 added,
0 changed and 0 destroyed.

### 7. Check the repository's OIDC subject format

Each role's trust policy matches the `sub` claim that GitHub writes into each token. Read
the format this repository uses before relying on those policies.

```bash
gh api repos/l3a0/marketlake/actions/oidc/customization/sub
```

The `sub` conditions in `infra/bootstrap/roles.tf` must start with the `sub_claim_prefix`
it returns. On 2026-10-06 it returned `use_immutable_subject: true`, which makes GitHub
write the owner's and the repository's numeric ids into each subject. A trust policy
written as `repo:l3a0/marketlake:*` would then match no token at all. A prefix that
differs from the code's needs a code change, and a role whose trust matches nothing fails
the plan job in step 10 at its credentials step.

### 8. Create the `infra` environment

Only the owner runs this step, and only the owner approves a deployment. A session never
approves, rejects or cancels a deployment, and never edits an environment or its
protection rules. That rule is the "Deployment approvals" section of
[`CLAUDE.md`](../CLAUDE.md#deployment-approvals-owner-directive-2026-10-06).

The environment must exist before the pull request merges. The merge that adds
`infra.yml` runs its apply job at once, and GitHub creates a missing environment with no
protection rules. Read the owner's numeric id first. It is `<github-user-id>` below.

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

1. `prevent_self_review` is off, because sessions merge as the owner, and the owner could
   not approve a run that their own merge started.
2. `can_admins_bypass` set to `false` is optional hardening. GitHub defaults it to
   `true`, which lets a repository admin deploy past the approval, and every session acts
   with the owner's admin token. On 2026-10-06 it was still `true`. Leave the line out to
   keep the default.
3. The reviewer is the owner alone.
4. The custom branch policy lets the next command limit deployments to `main`.

```bash
gh api -X POST repos/l3a0/marketlake/environments/infra/deployment-branch-policies -f name=main -f type=branch
```

### 9. Set the secrets and the variable

Nothing tracked names the account, the buckets or the user's policy, so the workflow
reads them from five settings.

1. The secret `AWS_APPLY_ROLE_ARN`, the apply role's ARN, the identifier AWS gives each
   resource. It sits on the `infra`
   environment, so the apply job can read it only after the approval.
2. The secret `AWS_PLAN_ROLE_ARN`, the plan role's ARN, on the repository.
3. The secret `TF_STATE_BUCKET`, the state bucket's name, on the repository.
4. The secret `BACKUP_BUCKET`, the backup bucket's name, on the repository.
5. The variable `BACKUP_POLICY_NAME`, the name of `marketlake-backup`'s inline policy, on
   the repository.

Each command below takes its value from AWS or from a placeholder, so no value is typed
into a file.

```bash
aws iam get-role --role-name marketlake-apply --query Role.Arn --output text --profile marketlake-admin | gh secret set AWS_APPLY_ROLE_ARN --env infra --repo l3a0/marketlake
```

```bash
aws iam get-role --role-name marketlake-plan --query Role.Arn --output text --profile marketlake-admin | gh secret set AWS_PLAN_ROLE_ARN --repo l3a0/marketlake
```

```bash
printf '%s' '<state-bucket>' | gh secret set TF_STATE_BUCKET --repo l3a0/marketlake
```

```bash
printf '%s' '<backup-bucket>' | gh secret set BACKUP_BUCKET --repo l3a0/marketlake
```

```bash
gh variable set BACKUP_POLICY_NAME --body '<policy-name>' --repo l3a0/marketlake
```

Then check the names against what the workflow reads. This lists every secret and
variable `infra.yml` names.

```bash
grep -oE '(secrets|vars)\.[A-Z_]+' .github/workflows/infra.yml | sort -u
```

The repository's secrets should include the three repository-scoped names.

```bash
gh secret list --repo l3a0/marketlake
```

The environment's secrets should include `AWS_APPLY_ROLE_ARN`.

```bash
gh secret list --env infra --repo l3a0/marketlake
```

The repository's variables should include `BACKUP_POLICY_NAME`.

```bash
gh variable list --repo l3a0/marketlake
```

### 10. Re-run the pull request's plan

The `tofu plan (live)` job stays red until steps 6 and 9 are done, because it refuses an
empty secret and cannot assume a role that does not exist yet. Find the pull request's
latest run of `infra.yml`.

```bash
gh run list --repo l3a0/marketlake --branch <branch> --workflow infra.yml --limit 1
```

Re-run its failed jobs, with the id the list printed in place of `<run-id>`.

```bash
gh run rerun <run-id> --failed --repo l3a0/marketlake
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
replaced. Any in-place update the summary shows is either adopted into code or listed on
the pull request's issue before the merge.

### 11. Merge, approve, and confirm nothing is left to change

Merge the pull request. Then the owner approves the `tofu apply (live)` run in the
Actions tab, outside 09:25 to 16:15 ET, the hours around the market session.

After the first green apply, two runs confirm that nothing is left to change. The first
is a manual run of `infra.yml` on `main`, which waits for the owner's approval like any
other apply. Its apply should find nothing to change.

```bash
gh workflow run infra.yml --repo l3a0/marketlake --ref main
```

The second is a laptop plan of `infra/bootstrap/` from `main`'s merge commit, in the same
worktree.

```bash
git fetch origin
```

```bash
git switch --detach origin/main
```

```bash
git log --oneline -1
```

The `-reconfigure` flag sets the backend up again from the file and never offers to move
state.

```bash
AWS_PROFILE=marketlake-admin tofu -chdir=infra/bootstrap init -reconfigure -backend-config="$HOME/.config/marketlake/infra/bootstrap.tfbackend"
```

```bash
AWS_PROFILE=marketlake-admin tofu -chdir=infra/bootstrap plan -var-file="$HOME/.config/marketlake/infra/bootstrap.tfvars"
```

It should report no changes. A change here means a review fix was pushed after the
bootstrap apply, or a stale checkout applied the bootstrap.

Then check the environment's protection.

```bash
gh api repos/l3a0/marketlake/environments/infra --jq '[.protection_rules[].type]'
```

It must list `required_reviewers` and `branch_policy`. An unprotected environment named
`infra` runs the same apply, and its run is just as green.

A later merge that touches `infra/` or `infra.yml` while a run waits starts a newer run.
After its approval, the older run checks `main` again, finds the newer commit, and skips
itself as stale. The newer run applies everything once the owner approves it. So
approving waiting runs out of order never applies an older commit last.

### 12. End the session

```bash
aws logout --profile marketlake-admin
```

An admin session lasts up to 12 hours, and anything on the laptop that sets the profile
can apply without approval until it ends. Then remove the worktree, from the main
checkout.

```bash
git worktree remove ~/marketlake-infra-<n>
```

## Recovery

A change made in the console shows up as a difference in the next plan, to be adopted
into code or reverted. A manual run of `infra.yml` from `main` reverts it behind the same
approval, with no code change.

A failed apply keeps the state of what it changed and releases its lock, so the next run
plans only what is left. A runner killed in the middle of an apply leaves its lock file,
`live/terraform.tfstate.tflock`, behind, and the next run's error prints the lock's id.
Clear it from an admin session, in a checkout of `main` initialised against
`live.tfbackend`.

```bash
AWS_PROFILE=marketlake-admin tofu -chdir=infra/live init -backend-config="$HOME/.config/marketlake/infra/live.tfbackend"
```

```bash
AWS_PROFILE=marketlake-admin tofu -chdir=infra/live force-unlock <lock id>
```

Any later change to `infra/bootstrap/*.tf` is applied from the laptop first, as in step
6, and its live apply is approved second. Apply it only from the bootstrap pull
request's branch at its final head, and only when its plan changes nothing that pull
request does not change. Every checkout reads the same backend file in
`~/.config/marketlake/infra/`, so a checkout that predates a merged change plans that
change away, and OpenTofu gives no warning. After the merge, plan the bootstrap from
`main`'s merge commit, as in step 11, and expect no changes.

## Bootstrap changes already known

Two open issues change `infra/bootstrap/`, so each one runs the order above: the laptop
applies the bootstrap first, and the owner approves the live apply second.

1. [#699](https://github.com/l3a0/marketlake/issues/699) adds an IAM user that writes the
   Schwab token to SSM Parameter Store. The apply role's grants name each resource it may
   write, so the bootstrap needs a grant on that user's ARN before the live apply can
   create the user.
2. [#704](https://github.com/l3a0/marketlake/issues/704) adds a second environment,
   `infra-auto`, for applies that need no click. The apply role's trust policy has to
   accept `environment:infra-auto` beside `environment:infra`, still only on
   `refs/heads/main`.
