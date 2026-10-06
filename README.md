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

Four steps set the bucket up. The owner does each by hand in the AWS console or CLI, and
nothing here names a real account, bucket, or key.

1. Create the bucket with versioning on and Object Lock off.
2. Add a lifecycle rule that expires noncurrent versions after 30 days under
   `manifest.jsonl`, `quarantine.jsonl`, `actions/` and `journal/`. Those are the files
   rewritten every night. Partitions keep every version, because with no Object Lock an
   overwritten partition's old version is its only good copy.
3. Create an access key whose policy grants exactly `s3:PutObject`, `s3:GetObject`,
   `s3:ListBucket` and `s3:GetBucketVersioning`, and nothing that deletes a version or
   changes the bucket.
4. Put the key in `config.yaml`, run the first upload, restore the whole lake once from
   the bucket with the `restore` command below, and only then change `backup_target`.

The examples below use the placeholder bucket `example-lake-backup` and keep the lake
under the `lake/` prefix, so the live check's probe objects can sit under `live-check/`
outside it. A lifecycle rule takes one prefix per filter, so step 2 is four rules.

```json
{
  "Rules": [
    {"ID": "manifest", "Status": "Enabled", "Filter": {"Prefix": "lake/manifest.jsonl"},
     "NoncurrentVersionExpiration": {"NoncurrentDays": 30}},
    {"ID": "quarantine", "Status": "Enabled", "Filter": {"Prefix": "lake/quarantine.jsonl"},
     "NoncurrentVersionExpiration": {"NoncurrentDays": 30}},
    {"ID": "actions", "Status": "Enabled", "Filter": {"Prefix": "lake/actions/"},
     "NoncurrentVersionExpiration": {"NoncurrentDays": 30}},
    {"ID": "journal", "Status": "Enabled", "Filter": {"Prefix": "lake/journal/"},
     "NoncurrentVersionExpiration": {"NoncurrentDays": 30}}
  ]
}
```

The key's policy for step 3:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketVersioning"],
     "Resource": "arn:aws:s3:::example-lake-backup"},
    {"Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObject"],
     "Resource": "arn:aws:s3:::example-lake-backup/*"}
  ]
}
```

Step 4's keys in `config.yaml`. The three `bucket_` keys may sit beside a path
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

Four commands go with it. The first two refuse with exit 2 on a shadow host, which is
any host whose config sets `role` to something other than `primary`. The restore runs on
either.

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
3. `uv run python -m lake.bucket restore <dest> --target s3://example-lake-backup/lake`
   downloads the current version of every object into `<dest>`, which must be empty or
   not exist yet, and verifies each file before `<dest>` is filled. A file the manifest
   records must match its latest entry, and any other file must match the SHA-256 S3
   stored when it was uploaded. A journal segment whose day the manifest records as
   compacted stays out, so the restored lake holds what the lake it came from holds. The
   download lands in `<dest>.restoring` beside it and is renamed onto `<dest>` only once
   every file has verified. A file that fails is named on its own line, `<dest>` is left
   as it was, and the command exits 1. Running it again resumes in `<dest>.restoring` and
   downloads only what is not already there and correct. It refuses with exit 2 when
   `<dest>` is not empty, which keeps it off a live lake, and when the disk beside it is
   too small. A year-end lake is about 154 GB. `<dest>` cannot be a mount point, since the
   finished directory is renamed onto it, so restore into a directory inside one. A
   restore uploads nothing and takes no lock, which is why a shadow host may run it, and
   it is how a new host's empty `lake_root` is seeded.
4. The nightly upload needs no command. Once `backup_target` names the bucket, the
   close+15 compaction uploads to it in place of `rsync`, and the Sunday job scrubs it and
   downloads the week's share of it to verify. A `shadow` host does neither.
   Compaction prints the upload's throughput to its log, in the line the first upload
   prints.

The restore brings back current versions only. When it names a file that failed, or a
file needs to be undone to an earlier state, the older version is recovered by hand in
the S3 console, because the narrow key holds no `s3:GetObjectVersion`.

1. Open the bucket, turn on **Show versions**, and go to the file's key under the lake's
   prefix.
2. Pick the latest version uploaded before the damage, and download it.
3. Check `shasum -a 256` of the download against the file's latest entry in the
   restored `manifest.jsonl`, which sits in `<dest>.restoring`.
4. Put the download at the file's path under `<dest>.restoring` and run the restore
   again. It finds the right hash there, skips the damaged current version, and finishes.

Versioning keeps every version of a partition and 30 days of the files rewritten nightly,
per the lifecycle rules above.

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

## Develop

The toolchain is [uv](https://docs.astral.sh/uv/). Set up the environment, then run
the linter and the test suite.

```bash
uv sync
uv run ruff check
uv run ruff format --check
uv run pytest
```

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
