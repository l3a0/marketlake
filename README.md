# Marketlake

Marketlake is a capture-first market data lake. It records full option chains and
equity quotes at one-minute cadence from the Schwab Trader API. Every snapshot not
taken is gone forever. So capture reliability is the first-order concern.

An unbuilt deliverable's issue is the source of truth for its scope. The design doc at
[docs/design.md](docs/design.md) carries the reasoning, the premise, and the
considered-and-rejected register, and it is the source of truth for everything already built.
The build plan at [docs/build-plan.md](docs/build-plan.md) records the slice build, deliverables
D0 through D21, and points at the MVP milestone that holds current work.

## Status

The slice build closed on 2026-10-05. Current work is the
[MVP 2](https://github.com/l3a0/marketlake/milestone/5) milestone, capture on a hosted VM.
The AWS resources it needs are code under `infra/`, written for
[OpenTofu](https://opentofu.org/) and applied from CI behind the owner's approval
([#664](https://github.com/l3a0/marketlake/issues/664)). Today that covers the backup
bucket, its IAM user, and the instance role the VM will use.

The first deliverable, D0, is the test harness. It builds the seams the whole suite
leans on. A seam is an injection point where a real dependency is swapped for a fake
one in a test. There are four seams and one builder.

1. An injected clock, so a test decides what time it is.
2. An injected calendar, so a test decides which sessions and half-days exist.
3. The vendor behind an interface, fed by recorded cassettes. A cassette is a saved
   vendor response replayed offline, so a test never touches the network.
4. The lake root as a temporary directory, so a test writes to a throwaway lake.
5. A fixture-lake builder, which assembles a known lake on disk for a test to read.

Three enforcement tests then stay in continuous integration for the life of the
project.

1. One fails the build on any direct clock call outside the clock module.
2. One fails the build on any hardcoded session time outside the calendar module.
3. One fails the build on any reference to the ntfy transport or the healthchecks
   pinger outside the outbox module.

## Layout

Production code lives under `src/lake`. Tests and their fakes live under `tests`.

- `src/lake/clock.py` is the clock module. It is the one place in production code
  that reads wall-clock time.
- `src/lake/calendar.py` is the calendar module. It is the one place in production
  code that names session times.
- `src/lake/vendor.py` and `src/lake/cassette.py` define the vendor interface and the
  cassette format.
- `src/lake/outbox.py` is the one place the ntfy transport and the healthchecks pinger
  are built. Under the config's `role: shadow` it builds recorders instead, which write
  each ping and page to `journal/outbox/` rather than sending it.
- `tests/support` holds the fakes, the fixture-lake builder, and the enforcement
  scanners.
- `infra/bootstrap` is the OpenTofu configuration CI needs before it can run: the bucket
  that holds the infrastructure's state, GitHub's OIDC provider, and the plan and apply
  roles. The owner applies it from the laptop.
- `infra/live` is the configuration CI applies: the backup bucket, its IAM user, and the
  instance role.
- `infra/ci` holds the two scripts `.github/workflows/infra.yml` runs. Each configuration
  keeps its own OpenTofu tests under `tests/`.

Tests sit in one folder per tier, matching the build plan's placement rule.

- `tests/unit` is decided from values alone with every seam faked. It holds the three
  enforcement guards.
- `tests/component` crosses exactly one real boundary: the real filesystem, or the
  real dependency behind a seam.
- `tests/integration` wires two or more subsystems through real boundaries. It is also
  where a test that needs a second real process lives.

## Back up to a bucket

The backup target is an external SSD by default, copied with `rsync`. It can instead be an
S3 bucket, which is what a hosted VM needs, since no SSD is attached to one. The design's
Backup section carries the reasoning, and
[#639](https://github.com/l3a0/marketlake/issues/639) carries the plan. Switching back is one
setting: put the path back in `backup_target`.

The bucket and the IAM user whose key the laptop uses are code in `infra/live/`, and
[Apply the infrastructure](#apply-the-infrastructure) below says how they are applied.
`infra/live/bucket.tf` holds the versioning, the encryption, the public-access block and
the four lifecycle rules. Each rule expires noncurrent versions after 30 days under one of
`manifest.jsonl`, `quarantine.jsonl`, `actions/` and `journal/`, the files rewritten every
night. Partitions keep every version, because with no Object Lock an overwritten
partition's old version is its only good copy. `infra/live/iam.tf` holds the user's policy,
which grants exactly `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` and
`s3:GetBucketVersioning`, and nothing that deletes a version or changes the bucket.

Two steps stay by hand, and nothing here names a real account, bucket, or key.

1. Create an access key for the user `marketlake-backup` in the AWS console. The key stays
   out of code, so no secret reaches the infrastructure's state.
2. Put the key in `config.yaml`, run the first upload, restore once from the bucket
   ([#640](https://github.com/l3a0/marketlake/issues/640)), and only then change
   `backup_target`.

The examples below use the placeholder bucket `example-lake-backup` and keep the lake
under the `lake/` prefix, so the live check's probe objects can sit under `live-check/`
outside it.

Step 2's keys in `config.yaml`. The three `bucket_` keys may sit beside a path
`backup_target`, which is how the first upload runs before the switch.

```yaml
bucket_access_key_id: <access key id>
bucket_secret_access_key: <secret access key>
bucket_region: <region, like us-east-2>
# Last, after the first upload and one restore have both passed:
backup_target: s3://example-lake-backup/lake
```

The client is built from those three values alone, never from `~/.aws/` or an `AWS_*`
environment variable. Loading `config.yaml` never checks them or the bucket's name, so
a mistyped value fails the backup, the first upload or the Sunday scrub that uses it,
each with one line naming the key, and never stops capture.

Three commands go with it. The first two refuse with exit 2 on a shadow host, which is
any host whose config sets `role` to something other than `primary`.

1. `uv run python -m lake.bucket live-check --target s3://example-lake-backup/live-check`
   confirms the four S3 behaviors the design rests on, and is live check 8 in the build
   plan. It writes three probe objects and names the prefix to delete by hand, since the
   narrow key cannot delete. The narrow key also cannot read an old version, so behavior
   3's read-back of the first version fails with it, and the check says to confirm in
   the console that the probe key shows two versions.
2. `uv run python -m lake.bucket first-upload --target s3://example-lake-backup/lake`
   uploads the whole lake, comparing every object, and prints its throughput. Run it on
   an evening after the 18:30 sweep. It does not run on Sunday from 19:55 to 23:30,
   while the Sunday job may be scrubbing the bucket, and a run begun just before 19:55
   stops when the window opens. Its last step holds the lake-root lock, and that step
   stops 30 minutes before the next session opens, so start it with the evening ahead of
   it. It refuses when the lake's own `manifest.jsonl` is empty or missing while the
   bucket's is not, which is what a wrong `lake_root` looks like. Each stop leaves the
   bucket's `manifest.jsonl` as it was, and running the command again picks up where it
   left off. The same command re-baselines a bucket whose
   copy of `manifest.jsonl` stopped being a prefix of the lake's, which the nightly
   upload refuses with a line naming it. `networkQuality -s`, built into macOS, measures
   upload capacity beforehand.
3. The nightly upload needs no command. Once `backup_target` names the bucket, the
   close+15 compaction uploads to it in place of `rsync`, and the Sunday job scrubs it.
   A `shadow` host does neither.
   Compaction prints the upload's throughput to its log, in the line the first upload
   prints.

## Reach the dashboard on a hosted VM

The dashboard binds the loopback address and serves only requests whose `Host` names
`localhost` or `127.0.0.1`, so on a VM it is unreachable from anywhere but the VM itself.
An SSH local forward reaches it from the laptop without opening another port. The forward
is a port on the laptop that ssh carries through its session to an address on the VM, so
it uses only the SSH port the VM already allows from the owner's address. The design's
dashboard section carries the reasoning, and
[#637](https://github.com/l3a0/marketlake/issues/637) carries the plan.

Nothing is installed on the VM for this. The forward needs only a running dashboard and
the SSH port. The dashboard will run on the VM under the systemd unit that
[#634](https://github.com/l3a0/marketlake/issues/634) adds. Until that lands nothing
listens on the VM's `127.0.0.1:8765`, and the forward reports the refusal described at
the end of this section. On the laptop, open the forward and leave it running:

```bash
ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=15 -L 127.0.0.1:8766:127.0.0.1:8765 <vm>
```

Then browse to `http://127.0.0.1:8766/`. Four choices in the command matter.

1. **The laptop side is `127.0.0.1:8766`.** While the laptop runs its own dashboard, that
   one holds `127.0.0.1:8765`. Given a bare port, ssh listens on every loopback address
   it can and counts the forward as working if any one of them binds. A forward on a bare
   8765 would still take `::1`, and `http://localhost:8765/` would then show the VM's
   page at the address of the laptop's. Naming one address makes a collision total, and
   `ExitOnForwardFailure=yes` turns it into an exit rather than a warning.
2. **The VM side names `127.0.0.1` rather than `localhost`.** The dashboard listens on
   IPv4 only. sshd would fall back to `127.0.0.1` after a refused `::1`, but naming the
   address spares that attempt and any dependence on the VM's `/etc/hosts`.
3. **`ServerAliveInterval=15`** makes ssh notice a network that vanished and close the
   forward within about a minute. Without it a dead forward can sit open for hours. The
   page marks itself stale only when its requests fail, so a forward that neither answers
   nor closes leaves the last data on screen unmarked
   ([#678](https://github.com/l3a0/marketlake/issues/678)). Once ssh has exited, after
   the laptop sleeps for example, run the command again and the page recovers on its own.
4. **`-N`** runs no remote command, so the session exists only to carry the forward.

The same forward as an entry in `~/.ssh/config`, with `marketlake-vm` as a placeholder
host name:

```text
Host marketlake-vm
    HostName <vm address>
    User <vm user>
    LocalForward 127.0.0.1:8766 127.0.0.1:8765
    ExitOnForwardFailure yes
    ServerAliveInterval 15
```

`ssh -N marketlake-vm` then opens it.

When the dashboard is not running on the VM, ssh prints `channel N: open failed: connect
failed: Connection refused` once for each connection the browser opens. A page already
open shows a banner saying the query service is unreachable, and clears it on its own at
the first refresh after the dashboard is back. Opened while the dashboard is down, the
page never loads, since the dashboard serves it, and the browser shows its own connection
error until a reload after the dashboard is back.

## Apply the infrastructure

The hosted deployment gets rebuilt at a cutover, after a VM dies, at a resize, and in a
disaster restore. With its AWS resources in code, each rebuild is one reviewed apply
instead of a walk through the console. The design's Deployment section carries the
reasoning, and [#664](https://github.com/l3a0/marketlake/issues/664) carries the plan.

The code is two OpenTofu configurations, each with its state in one S3 bucket under its
own key.

1. `infra/bootstrap/` holds what CI needs before it can run: the state bucket, GitHub's
   OIDC provider, and two roles that GitHub Actions assumes without a stored AWS key. The
   plan role reads and is trusted on pull requests. The apply role writes, and is trusted
   only in the `infra` environment on `main`. CI cannot apply the configuration that
   creates it, so the owner applies this one from the laptop.
2. `infra/live/` holds the backup bucket, its IAM user, and the instance role. CI applies
   it. `.github/workflows/infra.yml` plans it on each pull request from a branch here,
   and applies it after a merge to `main` once the owner approves the run.

Nothing tracked names the account, the buckets, or the user's policy. The workflow reads
them from five settings in the repository.

1. The secret `AWS_PLAN_ROLE_ARN`, the plan role's ARN.
2. The secret `TF_STATE_BUCKET`, the state bucket's name.
3. The secret `BACKUP_BUCKET`, the backup bucket's name.
4. The variable `BACKUP_POLICY_NAME`, the name of `marketlake-backup`'s inline policy.
5. The secret `AWS_APPLY_ROLE_ARN` on the `infra` environment, the apply role's ARN. The
   apply job reads it only after the approval.

### Tools and an admin session

The laptop applies the bootstrap and anything the apply role may not change, such as the
backup bucket's versioning or the instance role's trust. Both need an admin credential,
and it is kept apart from the default one. A plain `aws login` would write the `default`
profile, and then anything reading the default credential chain, an agent session's
`tofu apply` included, would act as account admin.

```bash
brew install awscli opentofu   # aws login needs AWS CLI 2.32.0 or later
aws login --profile marketlake-admin
export AWS_PROFILE=marketlake-admin
# ... the commands below ...
aws logout --profile marketlake-admin
```

CloudShell does not work for this. Its 1 GB home cannot hold the AWS provider.

The laptop keeps each configuration's inputs in `~/.config/marketlake/infra/`, outside
every checkout, because a fresh worktree has no ignored files. Four files sit there, with
placeholders in place of the real values.

1. `bootstrap.tfbackend` holds `bucket = "<state-bucket>"`.
2. `bootstrap.tfvars` holds `state_bucket = "<state-bucket>"`,
   `backup_bucket = "<backup-bucket>"` and `adopt_github_oidc_provider = false`.
3. `live.tfbackend` holds `bucket = "<state-bucket>"`.
4. `live.tfvars` holds `backup_bucket = "<backup-bucket>"` and
   `backup_policy_name = "<policy-name>"`.

A configuration is then planned and applied from the checkout's root like this:

```bash
tofu -chdir=infra/bootstrap init -backend-config="$HOME/.config/marketlake/infra/bootstrap.tfbackend"
tofu -chdir=infra/bootstrap plan -var-file="$HOME/.config/marketlake/infra/bootstrap.tfvars"
tofu -chdir=infra/bootstrap apply -var-file="$HOME/.config/marketlake/infra/bootstrap.tfvars"
```

### Values to read first

Three values are recorded nowhere, so the owner reads them from AWS before the first run.

1. The user's inline policy name, from
   `aws iam list-user-policies --user-name marketlake-backup`. It goes in `live.tfvars`
   and in the `BACKUP_POLICY_NAME` variable. The import needs it, so there is no safe
   guess.
2. The lifecycle rule ids and filter shape, from
   `aws s3api get-bucket-lifecycle-configuration --bucket <backup-bucket>`. The code uses
   the ids `manifest`, `quarantine`, `actions` and `journal`, each with a `Filter.Prefix`.
   A different id shows in the first plan as an in-place update.
3. Whether a GitHub OIDC provider exists, from `aws iam list-open-id-connect-providers`.
   An account holds one per issuer. When the list shows
   `token.actions.githubusercontent.com`, set `adopt_github_oidc_provider = true` in
   `bootstrap.tfvars`, so the bootstrap adopts it rather than failing to create a second.

### The first run, in order

The merge that adds `infra.yml` runs its apply job at once, and GitHub creates a missing
environment with no protection rules. So the environment exists before that merge.

1. Create the state bucket with
   `aws s3api create-bucket --bucket <state-bucket> --region us-east-1`. In `us-east-1` the
   command takes no `--create-bucket-configuration`. The bootstrap adopts the bucket and
   turns its versioning on.
2. Apply `infra/bootstrap/` from the pull request's branch, under the admin session.
3. Create the `infra` environment in the repository's settings, with the owner as required
   reviewer, `prevent_self_review` off, and deployments from `main` only. Then add its
   secret, `AWS_APPLY_ROLE_ARN`, from
   `aws iam get-role --role-name marketlake-apply --query Role.Arn --output text`.
4. Add the three repository secrets and the variable. `AWS_PLAN_ROLE_ARN` comes from the
   same command with `marketlake-plan`.
5. Re-run the pull request's `plan` job. Until steps 2 and 4 are done, that check is red
   by design. Its
   summary is the import plan, and it should destroy and replace nothing.
6. Merge, then approve the first apply.

After the first green apply, two plans confirm nothing is left to change: a manual run
of `infra.yml` from the Actions tab, whose apply finds nothing, and a laptop plan of
`infra/bootstrap/` from the merge commit. Then check the environment's protection.

```bash
gh api repos/l3a0/marketlake/environments/infra --jq '[.protection_rules[].type]'
```

It must list `required_reviewers` and `branch_policy`, because an unprotected environment
named `infra` runs the same apply just as green.

From then on, a change that touches both configurations, such as one that widens the
apply role, gets its bootstrap applied first and its live apply approved second.

### Which checkout applies the bootstrap

Every checkout reads the same backend file in `~/.config/marketlake/infra/`, so applying
`infra/bootstrap/` from a checkout that predates a merged change plans that change away,
and OpenTofu gives no warning. So:

- Apply the bootstrap only from its pull request's branch at its final head, after
  `git fetch`, and only when its plan changes nothing that pull request does not change.
- Treat a plan that destroys anything as coming from a stale checkout until shown
  otherwise.
- After every merge that touches `infra/bootstrap/`, plan it from `main`'s merge commit,
  and expect no changes. That plan is what shows a review fix pushed after the pre-merge
  apply, or a stale apply that dropped a Deny from the plan role.

### Drift and recovery

A change made in the console shows up as a difference in the next plan, to be adopted
into code or reverted. A manual run of `infra.yml` from `main` reverts it behind the same
approval, with no code change.

A failed apply keeps the state of what it changed and releases its lock. A runner killed
mid-apply leaves its lock file behind. Clear it from an admin session with
`tofu -chdir=infra/live force-unlock <lock id>`, where the id is the one the next run's
error prints.

## Develop

The toolchain is [uv](https://docs.astral.sh/uv/). Set up the environment, then run
the linter and the test suite.

```bash
uv sync
uv run ruff check
uv run ruff format --check
uv run pytest
```

The infrastructure under `infra/` needs OpenTofu, and applying it needs the AWS CLI. Both
come from Homebrew.

```bash
brew install awscli opentofu
```

`.github/workflows/infra.yml` runs these checks on every pull request that touches `infra/`
or the workflow, with no AWS credentials. The tests run against a mock AWS provider. The
first `init` downloads the AWS provider, about 750 MB unpacked, and setting
`TF_PLUGIN_CACHE_DIR` to a directory shares one copy between the two configurations.

```bash
tofu fmt -check -recursive infra/
tofu -chdir=infra/bootstrap init -backend=false -lockfile=readonly
tofu -chdir=infra/bootstrap validate
tofu -chdir=infra/bootstrap test
tofu -chdir=infra/live init -backend=false -lockfile=readonly
tofu -chdir=infra/live validate
tofu -chdir=infra/live test
```

Three things those checks cannot see are covered by `uv run pytest` instead.

1. `prevent_destroy` on each resource whose loss would lose backups.
2. The exact set of policies each bootstrap role carries.
3. The live backend's state key matching what the apply role may write.

### Keep development runs off the real config directory

`~/.config/marketlake/` holds the live Schwab token, and several commands default to it.
`python -m lake.reauth` with no `--token` writes the standard location, which is right
for the weekly ritual and wrong for anyone exercising the tool. On 2026-09-13 that is how
a stub reached the production token path and a working token was lost.

`MARKETLAKE_CONFIG_DIR` moves the whole directory for one process. Set it and the run
cannot reach the real token, the real `config.yaml`, or the real roster, whatever it is
given on the command line.

```bash
MARKETLAKE_CONFIG_DIR=/tmp/marketlake-dev uv run python -m lake.reauth
```

It has to be set before the process starts, because every default is built when the
module is imported. Exporting it in the shell being worked in covers that whole session.

Do not put it in a shell profile. The weekly re-auth runs in that same shell, so a
profile export would send the week's token to a throwaway directory while the daemon
kept reading the real one as it expired. The rendered `reauth.sh` unsets the variable to
make the ritual immune to this, and a re-auth run any other way prints the token path it
wrote, so the sign-off block is where to check it landed where you meant.

The test suite needs none of this, because `tests/conftest.py` covers it three ways. A
guard there fails any test that writes the real directory and names the path. That guard
is a monkeypatch, so it reaches no child process, and the same file therefore exports
`MARKETLAKE_CONFIG_DIR` at a throwaway directory when it is imported. A child inheriting
the suite's environment picks that up, whether or not the test that spawned it arranged
anything. Both of those are checks on the attempt, so the directory is also listed at the
start of a run and again at the end, and a run that changed it fails even when every test
passed.
