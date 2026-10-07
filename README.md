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
[OpenTofu](https://opentofu.org/) and applied from CI behind the owner's approval,
except the bootstrap that CI itself stands on, which the owner applies from the laptop
([#664](https://github.com/l3a0/marketlake/issues/664)). Today that covers the backup
bucket, its IAM user, the instance role, the IAM user that writes the Schwab token to the
VM's config parameters, and the VM itself
([#686](https://github.com/l3a0/marketlake/issues/686)). cloud-init takes a new VM from
nothing to a running daemon with no login, through `deploy/vm-bootstrap.sh`, and the VM
runs as a shadow beside the laptop until the cutover in
[#638](https://github.com/l3a0/marketlake/issues/638).

The control plane renders for both hosts: launchd jobs for the Mac, installed by hand, and
systemd units for a Linux VM, installed by `deploy/linux-install.sh`.

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
- `src/lake/aws_session.py` is the one place an AWS client is built, never from an
  `AWS_*` variable or `~/.aws/`. It builds the bucket's client and the token parameter's
  from `config.yaml` alone. Three clients can sign with the VM's instance profile: the
  bucket's, the token pull's, and the config render's, which takes its settings from the
  tracked `config/vm.yaml` because it writes `config.yaml`.
- `src/lake/token_store.py` carries the Schwab token to a hosted VM through an SSM
  parameter: the re-auth's put and the VM's pull.
- `src/lake/vm_config.py` writes the hosted VM's `config.yaml` from `config/vm.yaml`,
  four SSM parameters and the instance's `marketlake:backup-target` tag, refusing and
  keeping the old file when any input is wrong.
- `tests/support` holds the fakes, the fixture-lake builder, the enforcement scanners,
  and the proxy pool that measures a read's peak Arrow memory.
- `infra/bootstrap` is the OpenTofu configuration CI needs before it can run: the bucket
  that holds the infrastructure's state, GitHub's OIDC provider, and the plan and apply
  roles. The owner applies it from the laptop.
- `infra/live` is the configuration CI applies: the backup bucket, its IAM user, the
  instance role, the IAM user that writes the Schwab token, and the hosted VM.
  `infra/live/vm.tf` holds the VM, its security group, key pair and lake volume, and
  `infra/live/user-data.sh.tftpl` is the first-boot script it hands to cloud-init.
- `infra/ci` holds the two scripts `.github/workflows/infra.yml` runs. Each configuration
  keeps its own OpenTofu tests under `tests/`.
- `infra/README.md` is the owner's runbook for applying both configurations.
- `deploy/linux-install.sh` is the one install on a Linux host. It renders the systemd
  units from the checkout and installs them. `deploy/vm-bootstrap.sh` calls it at the
  VM's first boot, and every deploy calls it.
- `deploy/vm-bootstrap.sh` takes the VM from a fresh boot to a running daemon: it mounts
  the lake volume, installs `uv` and the units, renders `config.yaml`, pulls the token
  and applies the roster. It is safe to run again over SSH.
- `deploy/vm-empty-shadow-lake.sh` empties a shadow VM's lake so a restore can fill it.
  It refuses unless the lake volume is mounted, `role` is `shadow`, every unit is
  stopped and nothing is mounted below the lake root.
- `config/tickers.yaml` is the capture roster. A change to it is a reviewed pull request,
  and `python -m lake.roster apply` copies it onto a host.
- `config/vm.yaml` holds the VM's settings that are not secret, such as its `role` and
  `lake_root`, so a reviewed pull request is the one way they change.
- `.tool-versions` pins the `uv` version that CI and the VM's bootstrap install.

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
[#639](https://github.com/l3a0/marketlake/issues/639) and
[#640](https://github.com/l3a0/marketlake/issues/640) carry the plans for the upload and the
restore. Switching back is one setting: put the path back in `backup_target`. A path target
keeps its `rsync` copy, its scrub and its weekly restore test exactly as before.

The bucket and the IAM user whose key the laptop uses are code in `infra/live/`, and
[infra/README.md](infra/README.md) says how to apply them.
`infra/live/bucket.tf` holds the versioning, the encryption, the public-access block and
the four lifecycle rules. Each rule expires noncurrent versions after 30 days under one of
`lake/manifest.jsonl`, `lake/quarantine.jsonl`, `lake/actions/` and `lake/journal/`, the
files rewritten every night. Partitions keep every version, because with no Object Lock an overwritten
partition's old version is its only good copy. `infra/live/iam.tf` holds the user's policy,
which grants exactly `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` and
`s3:GetBucketVersioning`, and nothing that deletes a version or changes the bucket.

Two steps stay by hand on the key path, which is how the laptop signs its requests, and
nothing here names a real account, bucket, or key. A hosted VM takes its credentials from
an instance profile instead, and the procedure after the steps says what changes for it.

1. Create an access key for the user `marketlake-backup` in the AWS console. The key stays
   out of code, so no secret reaches the infrastructure's state.
2. Put the key in `config.yaml`, run the first upload, restore the whole lake once from
   the bucket with the `restore` command below, and only then change `backup_target`.

The lifecycle rules expect the lake under the `lake/` prefix, so `backup_target` ends in
`/lake`, which keeps the live check's probe objects under `live-check/` outside it. The
examples below use the placeholder bucket `example-lake-backup`.

Step 2's keys in `config.yaml`. On the key path the three `bucket_` keys may sit beside a
path `backup_target`, which is how the first upload runs before the switch.

```yaml
bucket_access_key_id: <access key id>
bucket_secret_access_key: <secret access key>
bucket_region: <region, like us-east-2>
# Last, after the first upload and one restore have both passed:
backup_target: s3://example-lake-backup/lake
```

On the key path the client is built from those three values alone, never from
`~/.aws/` or an `AWS_*` environment variable. Loading `config.yaml` never checks them,
`bucket_credentials` or the bucket's name, so a mistyped value fails the backup, the
first upload or the Sunday scrub that uses it, each with one line naming the key, and
never stops capture.

A hosted VM in the bucket's AWS account signs with short-lived credentials from an
instance profile, an IAM role attached to the instance, so no long-lived key sits on it
([#663](https://github.com/l3a0/marketlake/issues/663)). The bucket is the same, and the
two steps change as follows.

1. Step 1 has no key to create. `infra/live/iam.tf` describes the IAM role and instance
   profile `marketlake-instance`, whose S3 policies carry the same four actions as the
   laptop's key, split in two. The read half, `s3:ListBucket`, `s3:GetBucketVersioning`
   and `s3:GetObject`, is always on, so a restore runs on the VM with no stored key. The
   write half, `s3:PutObject` alone, stays off until the cutover turns it on, so a
   shadow VM holds no write credential to the primary's bucket
   ([#686](https://github.com/l3a0/marketlake/issues/686)). The VM also requires
   metadata tokens.
2. Step 2's key lines become one setting. The VM's `config.yaml` is written at deploy
   time, by [#686](https://github.com/l3a0/marketlake/issues/686)'s bootstrap and
   [#676](https://github.com/l3a0/marketlake/issues/676)'s deploy, from the tracked
   `config/vm.yaml`, the parameters
   [#699](https://github.com/l3a0/marketlake/issues/699) keeps in SSM Parameter Store,
   and the instance tag that names the bucket. The file names the source beside the
   region and holds neither key field.

   ```yaml
   bucket_credentials: instance_profile
   bucket_region: <region, like us-east-1>
   ```

3. The VM's `backup_target` names the bucket from its first boot, through that tag. The
   rest of step 2, the first upload and the restore, follows the cutover order on
   [#638](https://github.com/l3a0/marketlake/issues/638).
   Run `first-upload` only on the host whose lake the bucket should hold, because it
   replaces the bucket's `manifest.jsonl`. Run `live-check` in the same order, after the
   IAM role's write half is turned on, since the check writes probe objects.

The client asks the instance metadata service for credentials only when `config.yaml`
says `bucket_credentials: instance_profile`. It then takes them from that service alone,
never from `~/.aws/`, an `AWS_*` variable or a boto config file, and it sends the lookup
through no proxy. An absent
`bucket_credentials` means `keys`, the laptop's path. A host where the setting finds no
credentials, such as a VM with no instance profile or a laptop that carries the setting
by mistake, refuses with one line naming both fixes: attach the instance profile, or set
`bucket_credentials: keys` in `config.yaml`.

Four commands go with the bucket. The first two refuse with exit 2 on a shadow host, which is
any host whose config sets `role` to something other than `primary`. The restore runs on
either.

1. `uv run python -m lake.bucket live-check --target s3://example-lake-backup/live-check`
   confirms the four S3 behaviors the design rests on, and is live check 8 in the build
   plan. It writes three probe objects and names the prefix to delete by hand, since the
   bucket's credentials cannot delete. They also cannot read an old version, so behavior
   3's read-back of the first version fails with them, and the check says to confirm in
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
3. `uv run python -m lake.bucket restore <dest> --target s3://example-lake-backup/lake`
   downloads the current version of every object into `<dest>`, which must be empty or
   not exist yet, and verifies each file before `<dest>` is filled. A file the manifest
   records must match its latest entry, and any other file must match the SHA-256 S3
   stored when it was uploaded. A journal segment whose day the manifest records as
   compacted stays out, so the restored lake holds what the lake it came from holds. The
   download lands in a hidden working directory, `<dest>/.marketlake-restoring`, and its
   files are moved up into `<dest>` only once every file has verified, with
   `manifest.jsonl` moved last. A file that fails is named on its own line, `<dest>` gets
   no `manifest.jsonl`, and the command exits 1. Running it again resumes in the working
   directory and downloads only what is not already there and correct, and a run killed
   while moving files in finishes the move. Before moving anything it checks that each
   verified file is still there at its recorded size, and refuses when `<dest>` has
   gained a `manifest.jsonl` or a name it is about to move in, which is what a daemon
   started on that root looks like. It refuses with exit 2 when `<dest>` holds
   anything but `lost+found` and the working directory, which keeps it off a live lake,
   when `<dest>` is a symbolic link, and when its filesystem is too small. Two keys that
   differ only by case are named as failures, because on macOS one would overwrite the
   other. A year-end lake
   is about 154 GB. `<dest>` may be a volume's mount point, which is how a new host's
   empty `lake_root` is seeded. A restore uploads nothing and takes no lock, which is why
   a shadow host may run it.
4. The nightly upload needs no command. Once `backup_target` names the bucket, the
   close+15 compaction uploads to it in place of `rsync`, and the Sunday job scrubs it and
   downloads the week's share of it to verify. A `shadow` host does neither.
   Compaction prints the upload's throughput to its log, in the line the first upload
   prints.

The restore brings back current versions only, so a file that fails it is repaired by
hand. Which repair fits depends on whether the lake still holds a good copy.

While the lake is alive, the repair is the lake's own copy. The Sunday job's sample reads
only objects whose stored SHA-256 the scrub matched, so a sample mismatch is rot at rest
in an object whose stored checksum is right. A sealed partition is written once, so its
rotted current version is usually its only version, and neither upload replaces an
object whose stored checksum matches. Once the Sunday lake scrub passes on the file, put
it back with the bucket's credentials, as a single PUT carrying its SHA-256:

```bash
aws s3api put-object --bucket example-lake-backup --key lake/<path> --body <lake_root>/<path> --checksum-algorithm SHA256
```

When the lake is gone, an earlier version is recovered by hand in the S3 console. That
works only for a file the manifest records whose later version overwrote a good one,
such as a nightly file rewritten or a partition recompacted. The bucket's credentials hold
no `s3:GetObjectVersion`, so this is a console step.

1. Open the bucket, turn on **Show versions**, and go to the file's key under the lake's
   prefix.
2. Pick the latest version uploaded before the damage, and download it.
3. Check `shasum -a 256` of the download against the file's latest entry in
   `<dest>/.marketlake-restoring/manifest.jsonl`, which a failed restore leaves there.
4. Put the download at the file's path under `<dest>/.marketlake-restoring` and run the
   restore again. It finds the manifest's hash there and does not download the damaged
   current version. A file the manifest does not record is checked against the current
   version's stored checksum instead, so an earlier version of one never verifies.

Versioning keeps every version of a partition and 30 days of the files rewritten nightly,
per the lifecycle rules above.

## Install on a Linux host

A VM runs the control plane under systemd, and one tracked script installs it. Run it as
root from the checkout, which the owner account owns:

```bash
sudo deploy/linux-install.sh --owner <account> --lake-mount <path> [--config <path>]
```

`--lake-mount` is the lake volume's mount point, or `lake_root` when the lake sits on the
root volume. Every service waits for that mount. The script needs `uv` installed for the
owner at `~/.local/bin/uv`, and none of the files the jobs read. It is safe to run again,
and it restarts nothing that is running.

It renders the units afresh on every run into `<owner home>/.local/state/marketlake/systemd/`,
beside `install.sh`, `restart.sh` and `uninstall.sh`. A running service keeps its old unit
until it restarts:

```bash
sudo ~/.local/state/marketlake/systemd/restart.sh          # the dashboard only
sudo ~/.local/state/marketlake/systemd/restart.sh daemon   # the daemon, and a role change
sudo ~/.local/state/marketlake/systemd/restart.sh all      # both
```

A bare `restart.sh` restarts the dashboard alone, because restarting the daemon costs its
in-flight cycle. A role change in `config.yaml` reaches the daemon only through
`restart.sh daemon`.

New code does not wait for a restart. `lake` is an editable install, so once the checkout
moves or the install runs, the timer jobs, the compaction the daemon starts at close+15,
and any module the running daemon imports for the first time all run the new code. A
deploy therefore waits for the session's close before it updates the checkout or installs,
not only before it restarts ([#676](https://github.com/l3a0/marketlake/issues/676)).

Each unit logs to journald. The VM's clock runs in UTC, so read a unit's lines in Eastern
time:

```bash
TZ=America/New_York journalctl -u com.marketlake.daemon
```

On the hosted VM, `deploy/vm-bootstrap.sh` runs this install at first boot, then the
steps that follow it, in order. [The hosted VM](infra/README.md#the-hosted-vm) in the
infrastructure runbook says how to create the VM, rerun the bootstrap and restore its
lake. The design doc's Deployment section carries the reasoning for each unit setting.

## Carry the Schwab token to a hosted VM

The VM has no browser, so it cannot run the weekly Schwab login. The laptop's re-auth puts
the token into the SSM parameter `/marketlake/config/schwab-oauth-token`, and the VM copies
it into its own `token.json`. The design's Auth section carries the reasoning, and
[#636](https://github.com/l3a0/marketlake/issues/636) carries the plan.

One key in each host's `config.yaml` says what the host does with the token:

| `token_store` | `reauth.sh` writes | Used by |
| --- | --- | --- |
| absent or `file` | `token.json` | the laptop before the VM exists |
| `both` | `token.json`, then the parameter | the laptop once the VM exists |
| `store` | `token.json`, then the parameter | the VM |

Any other value prints one line naming it, writes `token.json`, and puts the parameter
when the keys below allow it. The put signs with three more keys, which hold the access key
of the IAM user that may only put this one parameter
([#699](https://github.com/l3a0/marketlake/issues/699)). They never fall back to the
`bucket_*` keys, whose user holds no grant to put it:

```yaml
token_store: both
token_store_access_key_id: <access key id>
token_store_secret_access_key: <secret access key>
token_store_region: <the bucket's region>
```

Under `both` or `store`, a missing or malformed key stops `reauth.sh` with exit 2 before the
browser opens. A put that succeeds adds a line to the sign-off block naming the parameter's
version number:

```text
  parameter:     /marketlake/config/schwab-oauth-token version <n>
```

The laptop has no grant to read the parameter, so that line is the only proof at the
terminal that it changed. Exit 3 means `token.json` was written and the parameter was not.
Its one line names the token's path and the fix. A refused or unknown key is fixed in the
`token_store_*` keys, and another login fails the same way until it is. Any other error is
fixed by running `reauth.sh` again.

The first `both` re-auth comes before the VM's first boot, in this order:

1. The put-only user from [#699](https://github.com/l3a0/marketlake/issues/699) exists,
   and its access key is in the three `token_store_*` keys in the laptop's `config.yaml`.
   The owner decided on 2026-10-06 not to create this key, and
   [#737](https://github.com/l3a0/marketlake/issues/737) replaces this step.
2. The laptop's checkout carries this code. `reauth.sh` runs the checkout's Python, and
   older code ignores `token_store`, writes the file, puts nothing, and exits 0 without a
   word.
3. Set `token_store: both`. Edit `config.yaml` outside a session, because capture reloads
   it every minute and a broken edit stops capture.
4. Run `reauth.sh` once, and check that it printed a version number.

To rotate the put-only key:

1. Put the new key in the `token_store_*` keys.
2. Run `reauth.sh`, and see the version line.
3. Deactivate the old key.

The VM copies the parameter with one command, run as the account that runs the daemon,
because a run as root leaves a `token.json` the daemon cannot read:

```bash
uv run python -m lake.token_store pull [--config <path>] [--token <path>]
```

It reads the parameter with the instance profile, as the bucket client does, and writes
`token.json` only when the local file is absent or unreadable, or the parameter was minted
later. It prints one line and exits with one of four codes:

1. 0 when it wrote the file or found it current.
2. 1 when the parameter is older, unusable, or minted more than an hour in the future, or
   when `token.json` could not be written.
3. 2 for a mistake in `config.yaml`.
4. 3 when the instance profile is not serving credentials yet, which is worth retrying.

The VM's first boot runs it
([#686](https://github.com/l3a0/marketlake/issues/686)), and
[#702](https://github.com/l3a0/marketlake/issues/702) runs it after that.

## Reach the dashboard on a hosted VM

The dashboard binds the loopback address and serves only requests whose `Host` names
`localhost` or `127.0.0.1`, so on a VM it is unreachable from anywhere but the VM itself.
An SSH local forward reaches it from the laptop without opening another port. The forward
is a port on the laptop that ssh carries through its session to an address on the VM, so
it uses only the SSH port the VM already allows from the owner's address. The design's
dashboard section carries the reasoning, and
[#637](https://github.com/l3a0/marketlake/issues/637) carries the plan.

Nothing is installed on the VM for this. The forward needs only a running dashboard and
the SSH port. The dashboard runs on the VM under its systemd unit,
`com.marketlake.dashboard.service`, which the Linux install places. While it is down
nothing listens on the VM's `127.0.0.1:8765`, and the forward reports the refusal
described at the end of this section. On the laptop, open the forward and leave it running:

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

## Change the roster

The roster says which tickers to capture. The repository tracks it as
`config/tickers.yaml`, so a change to it is a reviewed pull request. Every process on a
host still reads `~/.config/marketlake/tickers.yaml`, and
`python -m lake.roster apply` copies the tracked file there. It refuses a roster that
does not parse, one with no enabled ticker, a host with no `config.yaml`, and a run as
root. It replaces the host's file when the bytes differ, leaves it alone when they match,
and prints which. It never restarts the daemon, which reads the new roster on its next
cycle.

Before any write, and even when the bytes match, `apply` checks the roster against the
host's lake ([#692](https://github.com/l3a0/marketlake/issues/692)). It refuses a roster
that leaves out a ticker whose capture span is open, has that ticker disabled, or turns
its `options` off while the span records options. The refusal prints the entry to add or
the two ways to fix it, and leaves the host's roster as it was. It also refuses when a
reference file under `lake_root` cannot be read, or is missing on any host whose `role`
is not exactly `shadow`, since that means an unmounted or unrestored lake. A pass prints
how many open spans it checked. On a host whose `role` is exactly `shadow`, a missing
security master or capture spans file skips the check, and the skip says so.

On the VM, `deploy/vm-bootstrap.sh` runs it at first boot and on every rerun
([#686](https://github.com/l3a0/marketlake/issues/686)), and the post-close deploy
([#676](https://github.com/l3a0/marketlake/issues/676)) will run it once it is built.

On the laptop, run it from the main checkout after a `git pull`. Call the checkout's own
venv interpreter, the way the rendered `reauth.sh` does. `uv run` would sync the venv
the live daemon imports from before running anything.

```bash
git pull
.venv/bin/python -m lake.roster apply < config/tickers.yaml
```

Before the pull request that first adds `config/tickers.yaml` merges, check that it
matches the laptop's roster byte for byte, so the running daemon sees no change. `cmp`
prints nothing when the two files are equal.

```bash
cmp ~/.config/marketlake/tickers.yaml config/tickers.yaml
```

`lake.onboard` and `lake.retire` still write the host's copy, so a roster change is two
steps, and the order matters.

1. **To onboard,** merge the roster pull request after the close, then run
   `lake.onboard` the same evening. Capture keeps an enabled ticker the security master
   cannot resolve yet, so a forgotten onboard loses no minute.
2. **To retire,** run `lake.retire` after the close, then merge the roster pull request
   before 09:30 ET. A retire pull request merged before `lake.retire` has run is refused
   by the lake check, because the ticker's span is still open. A host that already has a
   roster keeps it and keeps capturing the ticker. A host with no roster yet, such as a
   rebuilt instance, gets nothing written, so its daemon stays down until the capture
   dead-man pages. Either way the refusal says to run `lake.retire` after the close and
   deploy again. The other order is loud too. Run first and left unmerged,
   the next apply puts the entry back, and the daemon pages during the session that an
   enabled ticker sits outside every span.

## Apply the infrastructure

The hosted deployment's AWS resources are code under `infra/`, in two OpenTofu
configurations. The owner applies `infra/bootstrap/` from the laptop, and
`.github/workflows/infra.yml` applies `infra/live/` after a merge to `main` once the owner
approves the run. [infra/README.md](infra/README.md) is the runbook, from the first
bootstrap through recovery, and lists the secrets and the variables the workflow reads.

## Develop

The toolchain is [uv](https://docs.astral.sh/uv/). `.tool-versions` pins the version CI
and the hosted VM install. Nothing checks it on the laptop, so a `uv` upgraded there by
Homebrew keeps working. Set up the environment, then run the linter and the test suite.

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

Six things those checks cannot see are covered by `uv run pytest` instead.

1. `prevent_destroy` on each resource whose loss would lose backups, captured minutes or
   the infrastructure's state, and on the token writer and its policy, which CI cannot
   delete.
2. The exact set of policies each bootstrap role and each live IAM user carries, and
   distinct names for the inline policies on one role.
3. The live backend's state key matching what the apply role may write.
4. No resource or data source that would store an SSM parameter's value in state.
5. The VM's `ignore_changes` list and the grants it waits for, the one zone its volume
   and subnet share, and the template that becomes its first-boot script taking only the
   owner and the volume id.
6. Every variable `infra/live/` requires reaching both plans in `infra.yml`, and the
   `replace_instance` input naming only the instance.

### Keep development runs off the real config directory

`~/.config/marketlake/` holds the live Schwab token, and several commands default to it.
`python -m lake.reauth` and `python -m lake.token_store pull` with no `--token` write the
standard location, which is right for the weekly ritual and the VM's pull and wrong for
anyone exercising either tool. On 2026-09-13 that is how a stub reached the production
token path and a working token was lost.

`MARKETLAKE_CONFIG_DIR` moves the whole directory for one process. Set it and the run
cannot reach the real token, the real `config.yaml`, or the real roster, whatever it is
given on the command line.

```bash
MARKETLAKE_CONFIG_DIR=/tmp/marketlake-dev uv run python -m lake.reauth
```

It has to be set before the process starts, because setting it part-way through a run
moves only what is resolved afterwards. Exporting it in the shell being worked in covers
that whole session.

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
