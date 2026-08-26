# Marketlake — DuckLake spike plan

Status: PLAN, 2026-08-26. Not run yet. Time box: one day.

The design doc pins DuckLake as the one component worth a slice-1 spike. This file is the executable version of that bar. It expands the pinned five drills into a run procedure. It adds two more. It also pins the alternatives that were considered and rejected, so they are not re-derived. The standing default is unchanged. If the spike does not clearly win, the \~40-line manifest protocol stays.

Read [design.md](design.md) first. The prior-art appendix carries the original five-drill bar. Everything here elaborates it and nothing here supersedes it.

## Contents

- [What this spike decides](#what-this-spike-decides)
- [The two layers, and which is in scope](#the-two-layers-and-which-is-in-scope)
- [Candidates considered](#candidates-considered)
- [Harness](#harness)
- [Drills, in run order](#drills-in-run-order)
- [The stop rule](#the-stop-rule)
- [Recording the verdict](#recording-the-verdict)
- [Sources](#sources)

## What this spike decides

One question. Does DuckLake strictly dominate the manifest protocol for the seal transaction?

DuckLake is the DuckDB team's lakehouse format. A lakehouse format is a specification for tracking which files make up a table. DuckLake stores the data as Parquet and stores all of the metadata in a SQL database, rather than in metadata files beside the data. That database is called the catalog. v1.0 shipped 2026-04 with a backward-compatibility guarantee. v1.1 is expected 2026-09.

Three outcomes are allowed.

- **Replace.** Every drill passes and measured net code drops. Adopt.
- **Augment.** The crash drills pass, but a sha256 sidecar or an extra backup step survives. DuckLake then sits beside existing machinery instead of deleting it.
- **Reject.** Any crash-consistency or restore miss. Keep the manifest protocol.

Augment is not a win. The design already carries the manifest, and it works. A catalog that only adds must still earn its dependency on an un-buy-backable dataset. Treat Augment as Reject unless it closes the journal-less-surface orphan window outright, which is the one thing the manifest protocol cannot do.

The predicted outcome is Augment, because the sha256 sidecar and the offline verify most likely survive drills 2 and 3. That prediction is recorded here so the spike can contradict it.

## The two layers, and which is in scope

Marketlake has two write boundaries with opposite requirements. Testing the wrong one is the main way this spike wastes its day.

**The journal is out of scope.** Capture appends Arrow IPC record batches every minute. Each cycle is made durable with `fcntl F_FULLFSYNC`. That call forces the drive to flush its own write cache, which plain `fsync` on macOS does not. The layer needs durability and torn-tail recovery. It does not need atomicity. A cycle writes independent segments, one per surface and ticker. A crash mid-cycle leaves one ticker's batch durable and another's not. The gap-marking machinery already covers exactly that, per surface and per ticker. So there is no cross-file invariant to violate. The journal is therefore already a write-ahead log. A write-ahead log is a log made durable before the data it describes counts as committed. The journal has that durability and it has a recovery rule. The only thing it lacks is atomicity across surfaces, which nothing in this design wants.

**The seal transaction is in scope.** Compaction writes the day's Parquet partition, appends the manifest entry, then unlinks the segments. That is a multi-step mutation across files with a real crash window. It is what the manifest protocol pays for. The journal-less surfaces, `bars/` and `actions/`, carry the sharper version of the problem. A crash between partition write and manifest append leaves a permanently invisible orphan. Catching that is the reverse scrub's whole reason to exist. Turning that detectable failure into an impossible one is the specific prize.

## Candidates considered

DuckLake is the only one being spiked. The rest are pinned here with reasons.

| Option | The case for it | The case against |
| --- | --- | --- |
| **DuckLake** | Keeps the Parquet plus DuckDB substrate. ACID across multi-table commits. Time travel. v1.0 is production-tagged. | Metadata lives in a SQL database, not in the file tree. That is precisely what drills 2 through 4 attack. |
| **Apache Iceberg** | Metadata lives in the tree as JSON and Avro files. A bare-disk copy is self-describing with no live catalog. Most likely of any candidate to still be readable in ten years. | DuckDB only reads Iceberg without a REST catalog. A REST catalog is a metadata service reached over HTTP, such as Polaris or Nessie. Writes require one, which is server-shaped and out under the rule that cut QuestDB. Writing instead through PyIceberg, the Python Iceberg library, splits the write and read paths across two libraries. |
| **Delta Lake** | Same in-tree log advantage, under `_delta_log/`. DuckDB reads it. | No DuckDB write path. Ecosystem is Databricks-shaped. A worse Iceberg for this use. |
| **Lance** | Versioned columnar format with in-tree manifests and cheap versioning. | Not Parquet. That kills the read-with-any-tool-forever property, which is the rule that already cut ArcticDB. |
| **Hand-rolled SQLite catalog** | A real transaction around the seal. Keeps hive paths and keeps sha256. Hive paths are directory names carrying `key=value` pairs. | The manifest stops being greppable text that restores with the tree. It adds a binary file to the backup that must restore consistently. A wash on net code. |
| **Keep the manifest** | Zero new dependencies. A text ledger that restores with the lake. Already review-hardened. | The \~40 lines stay. The orphan window on the journal-less surfaces stays detectable rather than impossible. |

**Iceberg is the named fallback, and only conditionally.** It beats DuckLake on exactly one axis this design prices, which is offline verifiability of a bare backup disk. Open it only if DuckLake fails drill 2 or drill 3, since in-tree metadata is the specific fix for those two failures. Do not run it as a parallel spike. Component minimalism is load-bearing here.

**Rejected for the journal, so it is not re-proposed.** SQLite in write-ahead-log mode is the only serious ACID alternative to Arrow IPC segments. It was rejected. It puts a non-columnar format on the un-buy-backable capture path, adds an export step at compaction, and buys atomicity the design does not need. LMDB and RocksDB are the same trade with worse self-description. A live DuckDB database file is worse still, because single-writer file locking fights the read-during-capture story the dashboard wants. The one cheap hardening worth keeping in view is a CRC32C checksum per record batch, which makes a torn tail detected positively rather than by parse failure. CRC32C is a cyclic redundancy check, a short value computed over bytes to detect corruption. That is a few lines, not a format change, and it is independent of this spike.

## Harness

Budget roughly ninety minutes. Point everything at a scratch lake. Never point a spike at the capture lake.

**Build the fixture at real scale.** The sizing section puts a SPY chain at 10 to 25k contracts per snapshot across \~405 snapshots. Use the low end and generate a \~4M-row ticker-day. This matters more than it looks. A thousand-row fixture commits too fast to land inside a crash window, and the spike then concludes that everything passes.

**Do not race a `kill -9`.** Use deterministic crash points driven by an environment variable. At each point the code calls `os.abort()`. That raises SIGABRT and skips Python's exit handlers, buffer flushes, and destructors, so it has genuine process-death fidelity. Three points are needed: `mid_data_file_write`, `post_data_file_pre_commit`, and `post_commit_pre_unlink`.

**Be honest about what each fidelity tier tests.** The three tiers are not interchangeable.

| Tier | Method | Cost | What it catches | What it cannot catch |
| --- | --- | --- | --- | --- |
| A. Process death | `os.abort()` at a crash point, on the Mac | Minutes per run | Logic bugs: visible partial state, non-idempotent re-runs, missing cleanup | Anything about stable storage |
| B. Power loss | Linux VM under Lima or UTM, lake on a second virtual disk, VM process hard-killed | Half a day | Whether a commit actually reached the disk | Reordering below the block layer |
| C. Block replay | `dm-log-writes` | Multi-day | Everything, at barrier granularity | Nothing relevant |

Tier A is where most of the value is. It runs on the Mac and it catches logic bugs. It cannot test durability at all, because process death leaves the operating system's page cache intact. The page cache is the kernel's in-memory copy of recently written file data. Everything already handed to `write()` survives a killed process, so Tier A never exercises the flush. Tier B loses the guest page cache and does exercise it. Lima and UTM are virtual-machine runners for macOS. Tier C uses `dm-log-writes`, a Linux device-mapper target that records every write and flush and replays the stream to any chosen point. It is what filesystem developers use, and it is a multi-day setup.

**Tier C is cut. Tier B is conditional.** Run Tier A for every drill. Cover durability with drill 6's syscall trace instead, which takes minutes. Escalate to Tier B only if that trace is inconclusive. This is the only shape that fits the one-day box.

## Drills, in run order

The design doc numbers five drills. Those numbers are kept, so cross-references still mean what they meant. Drills 6 and 7 are new. The running order below is cheapest-disqualifier-first, not numeric. Three drills can disqualify DuckLake inside the first hour, before the harness exists.

### Drill 6, catalog durability (new)

The entire design rests on `F_FULLFSYNC`, because plain `fsync` on macOS returns before the drive's write cache is flushed. If DuckLake's commit lands in a catalog that uses a weaker barrier, the new ACID commit is *less* durable than the manifest line it replaces.

Procedure. Check `PRAGMA fullfsync` and `PRAGMA synchronous` on the connection DuckLake opens for its catalog. Check whether they can be set. If the connection is not reachable, trace one commit with `sudo fs_usage -w -f filesys`. `fs_usage` is the macOS system-call tracer. Look for the `F_FULLFSYNC` `fcntl`.

Pass bar. The commit issues `F_FULLFSYNC`, or it can be made to.

Why this runs first. A miss here is invisible and permanent. It also costs minutes.

### Drill 2, offline integrity

The backup copy must stay verifiable on a bare disk with no live catalog.

Procedure. Read the catalog's own metadata tables. `ducklake_data_file` is the table listing each Parquet file the catalog tracks. Check whether it carries a content-hash column.

Pass bar. Emit a `SHA256SUMS` file from the catalog and have `shasum -c SHA256SUMS` return zero on a bare copy of the data with no DuckDB installed.

If it fails. A sha256 sidecar keeps being written by hand. That is the Augment branch, and it is most of the reason Augment is the predicted outcome.

### Drill 3, backup and restore

The plain-rsync backup property must survive.

Procedure. Do not copy a quiescent lake. Start a long compaction, then `rsync -a` the whole tree while it runs, with traversal ordered so rsync reaches the catalog last. The failure to hunt is specific. rsync grabs the catalog after the commit but the data files from before the new file existed. The restored lake then references a file that is not there.

Pass bar. On the restored copy, every table answers `SELECT count(*)`, and every path in `ducklake_data_file` exists on disk.

If it fails. The fix is a catalog snapshot step before the rsync, such as SQLite's `VACUUM INTO`, which writes a consistent copy of a live database to a new file. That is an addition and it counts against drill 5.

### Drill 1, crash consistency

An aborted transaction must leave zero visible state. It must also close the journal-less-surface orphan window.

Procedure, part one. Crash at `post_data_file_pre_commit`. Reopen. Assert zero rows for that ticker-day and no new snapshot. Then count the unreferenced Parquet files left on disk. Record that number. Uncommitted data files are orphan debris, and cleaning them needs its own routine, which counts against drill 5.

Procedure, part two. Crash at `post_commit_pre_unlink`. Reopen. Assert the row count matches the fixture. Then re-run compaction for the same ticker-day and assert the count did **not** double. This is the assertion most likely to fail, because a naive `INSERT INTO ... SELECT` re-run doubles the day.

Procedure, part three. Repeat the whole thing against a single `bars/` partition. Assert there is no reachable state where the file exists and the table does not see it. Closing that window is drill 1's second job and the spike's actual prize.

Pass bar. Twenty loops per crash point, zero failures. A bug that fires one run in ten is the entire reason to loop.

### Drill 4, catalog loss

Catalog loss may cost metadata. It may never cost data.

Procedure. Delete the catalog. Run `SELECT count(*) FROM read_parquet('<scratch-lake>/**/*.parquet', union_by_name := true)` and compare against the fixture's row count.

Two cheap sub-questions belong here, because both are decision-relevant.

- Does a partitioned DuckLake table write human-readable hive paths, or opaque names in one flat directory? If it writes hive paths, the pinned cost of a catalog-managed layout mostly evaporates. That could flip the decision on its own, so check it even though the drill does not require it.
- Does any Parquet file hold rows the catalog would have masked? Under append-only, no. Under any `UPDATE` or `DELETE`, yes, because those are recorded as delete files rather than by rewriting data. A delete file is a side file marking rows as removed. A catalog-loss reconstitution would then silently over-report.

Pass bar. Exact row-count match.

Adoption condition, and it is hard. **Never `UPDATE`. Never `DELETE`.** The immutability rule already says this. Write it down as an explicit DuckLake precondition anyway, because the failure mode is silent over-reporting rather than an error.

### Drill 5, net code

Deletions must exceed additions.

Procedure. Do not eyeball it. Write both compaction paths for real in the scratch tree and count the lines.

Deletions claimed: the manifest writer, torn-line handling, verify-then-delete, and the reverse scrub.

Additions actually incurred: catalog attach and version pin, the idempotency guard from drill 1, the sha sidecar if drill 2 failed, the catalog snapshot step if drill 3 failed, orphan data-file cleanup, and a catalog line in the restore runbook.

Pass bar. A measured negative line delta.

Why this runs last. Drills 2 and 3 are the drills that generate the additions. Counting before they run measures a fantasy.

### Drill 7, version churn (new)

An un-buy-backable dataset cannot depend on a format that might need a migration you cannot run.

Procedure. v1.0 landed 2026-04 and v1.1 is expected 2026-09, so this spike runs roughly three weeks before a format-version bump. Pin and record the exact DuckDB and DuckLake versions used. Read v1.0's backward-compatibility promise and confirm that a v1.1 reader opens a v1.0 catalog.

Pass bar. A written compatibility promise that covers the bump, plus recorded version pins.

## The stop rule

Set an actual timer for one day. When it rings, whatever state the drills are in is the verdict. A replacement that needs coaxing past the time box has already lost. That rule is inherited from the design doc and it is not negotiable inside the spike.

## Recording the verdict

Append the outcome to the design doc's prior-art appendix, in the DuckLake bullet, as one of Replace, Augment, or Reject, with the date and the version pins from drill 7. Record the drill that decided it. If the verdict is Reject or Augment, that bullet becomes a considered-and-rejected entry in the doc's own register, and this plan file stays as the evidence behind it. If any drill produced a surprise that changes the manifest protocol's own design, that change is a separate proposal and a separate review.

## Sources

Verified 2026-08-26.

- [DuckLake v1.0 release](https://ducklake.select/2026/04/13/ducklake-10/) states the April 2026 production tag and the backward-compatibility guarantee.
- [DuckLake FAQ](https://ducklake.select/faq) states that the catalog may be any SQL database with ACID operations and primary keys, and that data files must be Parquet. It does not cover offline verification, backup, or catalog loss. Those are exactly what drills 2 through 4 must answer first-hand.
- [Writing to Iceberg (DuckDB)](https://duckdb.org/docs/current/core_extensions/iceberg/writing) and [Iceberg REST Catalogs (DuckDB)](https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_rest_catalogs) document that DuckDB's Iceberg writes require an attached REST catalog.
- [PyIceberg configuration](https://py.iceberg.apache.org/configuration/) documents the SQL catalog backed by SQLite with a local-filesystem warehouse, which is the only laptop-shaped Iceberg write path.
