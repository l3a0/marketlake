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
2. `infra/live/` holds the backup bucket, its IAM user `marketlake-backup`, the instance
   role `marketlake-instance` with its read of the config parameters, and the IAM user
   `marketlake-token-writer`, which writes the Schwab token's parameter.
   `.github/workflows/infra.yml` plans it on each pull request from a branch here, and
   applies it after a merge to `main` once the owner approves the run.

The laptop also applies any change the apply role may not make, such as the backup
bucket's versioning or a role's trust policy. A trust policy says who may assume the
role, that is, take on its permissions.

This repository is public, so every value below appears as a placeholder. Three values
stay out of every tracked file, every issue and every comment.

1. The account id, which step 2 prints and no command below needs.
2. `<state-bucket>`, the state bucket's name.
3. `<backup-bucket>`, the backup bucket's name.

`<policy-name>` is the name of `marketlake-backup`'s inline policy. It lives in a
repository variable, which GitHub does not mask in logs, so it is not a secret. It is
still written only on the laptop and in GitHub's settings.

The rest are read when they are needed.

- `<github-user-id>` is the owner's numeric GitHub id, which step 8 reads.
- `<branch>` and `<n>` are the pull request's branch and number.
- `<run-id>` and `<lock-id>` are ids that an earlier command prints, named where each one
  appears.

Every placeholder on a command line sits inside double quotes, because a bare `<x>` left
unreplaced is a redirection in zsh.

## The first run, in order

This section records the run for
[PR #698](https://github.com/l3a0/marketlake/pull/698), merged 2026-10-06, which added
`infra.yml`. A rerun with `infra.yml` already on `main`, such as a rebuild into a fresh
account, differs in three steps.

1. Step 7 applies the bootstrap from a worktree at `origin/main` rather than at a pull
   request's branch.
2. Step 10 drops out, since no pull request needs a plan.
3. Step 11 replaces the merge with a manual run of `infra.yml` on `main`, which is
   `gh workflow run infra.yml --repo l3a0/marketlake --ref main`.

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

List the user's inline policies. The one name it prints is `<policy-name>`. An import
adopts a resource that already exists into the state, rather than creating it. The import
of the user's policy needs that name, so there is no safe guess.

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

`live.tfvars` holds the two values the live configuration needs.

```bash
cat > ~/.config/marketlake/infra/live.tfvars <<'EOF'
backup_bucket      = "<backup-bucket>"
backup_policy_name = "<policy-name>"
EOF
```

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

### 9. Set the secrets and the variable

Nothing tracked names the account, the buckets or the user's policy, so the workflow
reads them from five settings.

1. The secret `AWS_APPLY_ROLE_ARN`, the apply role's ARN, the identifier AWS gives each
   resource. It sits on the `infra` environment, so the apply job can read it only after
   the approval.
2. The secret `AWS_PLAN_ROLE_ARN`, the plan role's ARN, on the repository.
3. The secret `TF_STATE_BUCKET`, the state bucket's name, on the repository.
4. The secret `BACKUP_BUCKET`, the backup bucket's name, on the repository.
5. The variable `BACKUP_POLICY_NAME`, the name of `marketlake-backup`'s inline policy, on
   the repository.

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

Then check the names against what the workflow reads. This lists every secret and
variable `infra.yml` names.

```bash
grep -oE '(secrets|vars)\.[A-Z_]+' "$HOME/marketlake-infra-<n>/.github/workflows/infra.yml" | sort -u
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

## The config parameters

The hosted VM's `config.yaml` is written at deploy time from SSM Parameter Store, so a VM
can be rebuilt and a secret rotated without logging in to it
([#699](https://github.com/l3a0/marketlake/issues/699)). Five `SecureString` parameters
sit under `/marketlake/config/`. Each of the first four is the `config.yaml` key it fills,
written in kebab case.

1. `/marketlake/config/schwab-api-key`, for `schwab_api_key`.
2. `/marketlake/config/schwab-app-secret`, for `schwab_app_secret`.
3. `/marketlake/config/healthchecks-ping-key`, for `healthchecks_ping_key`.
4. `/marketlake/config/ntfy-topic`, for `ntfy_topic`.
5. `/marketlake/config/schwab-oauth-token`, the contents of `token.json`.

The code manages no parameter and names only the path. A parameter resource would read
the decrypted value into the state on every refresh, so the owner puts each value from
the laptop. `marketlake-instance` reads the path, and neither CI role can. Once
[#695](https://github.com/l3a0/marketlake/issues/695) attaches AWS's
`AmazonSSMManagedInstanceCore`, the instance role can read every parameter in the
account, which holds no other secret. `marketlake-token-writer` can only overwrite the
token, which the laptop's weekly re-auth does once
[#636](https://github.com/l3a0/marketlake/issues/636) lands.

### Create the token writer, in order

The pull request that adds the token writer changes the bootstrap too, so it follows
[Changing the bootstrap](#changing-the-bootstrap).

1. Apply the bootstrap from the laptop, from the pull request's branch at its final head,
   as in step 7. That gives the apply role `iam:CreateUser` and `iam:PutUserPolicy` on
   `marketlake-token-writer`.
2. Merge, then approve the `tofu apply (live)` run, which creates the user and its
   policy.
3. Create an access key for `marketlake-token-writer` in the AWS console. The key goes
   into the laptop's `config.yaml` under
   [#636](https://github.com/l3a0/marketlake/issues/636)'s `token_store_*` keys, and it
   stays out of code, so no secret reaches the state.

Never create the user by hand. The live apply creates it, and its `CreateUser` fails with
`EntityAlreadyExists` when the user already exists.

### Put the values

Every command below passes `--profile marketlake-admin` and `--region us-east-1`. The CI
roles' Deny and the VM's read both name that region, so a parameter put anywhere else is
readable by the plan role and out of the VM's reach. Each passes `--tier Standard`,
because a parameter once put as Advanced never moves back to Standard, and
[#636](https://github.com/l3a0/marketlake/issues/636)'s re-auth puts the token as
Standard, so its puts would then fail. Each also passes `--overwrite`, so a re-run
replaces the value instead of failing with `ParameterAlreadyExists`.

Each of the four secrets is read from the keyboard into a variable. A value typed on the
command line lands in shell history. A value kept in a file picks up the editor's
trailing newline, and the put stores that newline as part of the secret. `read -rs`
echoes nothing and writes nothing to disk, and `unset` drops the value after the put.
Each command prints a prompt. Paste the value and press Enter. The prompt comes from
`printf`, because zsh's `read -p` means a coprocess and leaves the variable empty. The
variable lives in one command line, because a command run through Claude Code's `!`
prefix may start a fresh shell.

```bash
printf 'Schwab API key: '; read -rs V && echo && aws ssm put-parameter --name /marketlake/config/schwab-api-key --value "$V" --type SecureString --tier Standard --overwrite --profile marketlake-admin --region us-east-1; unset V
```

```bash
printf 'Schwab app secret: '; read -rs V && echo && aws ssm put-parameter --name /marketlake/config/schwab-app-secret --value "$V" --type SecureString --tier Standard --overwrite --profile marketlake-admin --region us-east-1; unset V
```

```bash
printf 'Healthchecks ping key: '; read -rs V && echo && aws ssm put-parameter --name /marketlake/config/healthchecks-ping-key --value "$V" --type SecureString --tier Standard --overwrite --profile marketlake-admin --region us-east-1; unset V
```

```bash
printf 'ntfy topic: '; read -rs V && echo && aws ssm put-parameter --name /marketlake/config/ntfy-topic --value "$V" --type SecureString --tier Standard --overwrite --profile marketlake-admin --region us-east-1; unset V
```

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

### Recover from a leaked token-writer key

The key can only overwrite the token parameter, so the response is short.

1. Deactivate the key for `marketlake-token-writer` in the AWS console.
2. Run the `describe-parameters` check above.
3. If the token's tier reads `Advanced`, delete the parameter and put the token again as
   above.

```bash
aws ssm delete-parameter --name /marketlake/config/schwab-oauth-token --profile marketlake-admin --region us-east-1
```

A forged token put with a future mint time would still reach the VM and stop capture
until a login there. The guard against that belongs to
[#636](https://github.com/l3a0/marketlake/issues/636)'s pull.

## Bootstrap changes already known

Two open issues change `infra/bootstrap/`, and each follows the order under
[Changing the bootstrap](#changing-the-bootstrap). Each issue carries its own scope.

1. [#676](https://github.com/l3a0/marketlake/issues/676) adds a deploy role.
2. [#704](https://github.com/l3a0/marketlake/issues/704) widens the apply role's trust to
   a second environment.
