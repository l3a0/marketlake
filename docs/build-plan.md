# Marketlake — build plan

This plan sequences the build. It says what ships, in what order, and how each piece is tested. It does not restate the design, and it does not carry a deliverable's scope. Each unbuilt deliverable's issue is the source of truth for what that deliverable is, per the directive in `CLAUDE.md`. The design doc at [design.md](design.md) carries the reasoning and the considered-and-rejected register. Read the issue for scope and the design doc for why.

Status: PLAN, 2026-08-26.

## Slicing rule

A slice is a deliverable that ships on its own. Three tests decide whether something qualifies.

1. It is testable with the market closed and the network off.
2. It stands alone as a shippable unit.
3. It does not delay the capture clock.

The capture clock is the daily job that records the market. Nothing in a later slice may push that job later or make it less reliable.

## Deliverables

The build is a sequence of deliverables, D0 through D21. Each is one focused unit of work. They group into the five slices from the design doc, plus the test harness that comes first.

An entry that leads with an issue number rather than a D number is work a deliverable did not finish. The issue is the source of truth for it, and the entry says why the work belongs to this slice. Neither repeats the other: an entry gives the sequencing reason, and never records whether the work has since been done, which is the issue's to say. Where an entry and its issue disagree, the issue is right. That holds for a D-numbered entry naming an unbuilt deliverable too, not only for an issue-led one. An entry that claimed both would go stale the moment the work landed, which is what happened while the entries were marked unowned.

The same rule now covers the unbuilt deliverables themselves. D16 through D21 are links, because an issue holding a deliverable's scope beside its status is one place to read rather than two to reconcile.

### D0, the test harness

D0 builds the seams the whole suite leans on. A seam is an injection point where a real dependency is swapped for a fake one in a test. There are four seams and one builder.

1. An injected clock, so a test decides what time it is.
2. An injected calendar, so a test decides which sessions and half-days exist.
3. The vendor behind an interface, fed by recorded cassettes. A cassette is a saved vendor response replayed offline, so a test never touches the network.
4. The lake root as a temporary directory, so a test writes to a throwaway lake.
5. A fixture-lake builder, which assembles a known lake on disk for a test to read.

Two enforcement tests then stay in continuous integration for the life of the project. Continuous integration is the service that runs the suite on every push.

1. One fails the build on any direct clock call outside the clock module.
2. One fails the build on any hardcoded session time outside the calendar module.

These two keep the seams from being bypassed later.

### Slice 1, the capture clock

Slice 1 starts the capture clock on the real dataset. It is D0 through D8.

- **D1** config and paths.
- **D2** session clock.
- **D3** security master.
- **D4** journal segment writer.
- **D5** vendor client and cassettes.
- **D6** manifest ledger and the flock. The flock is the kernel file lock that serializes lake writes, defined in the design doc.
- **D7** the capture primitive, one cycle. One cycle is auth, then fetch chain and quotes, then journal, then manifest.
- **D8** the slice-1 launchd runner, the onboarding command, and the day-one measurements. launchd is macOS's built-in job scheduler. The day-one measurements are the first sessions of fetch-latency and sizing data. The design doc's guard constants are tuned against them.

### Slice 2, the daemon

Slice 2 wraps the primitive in the market-hours loop and hardens it for a laptop.

- **D9** daemon loop. It fires the capture cycle at each session minute and idles otherwise. It holds no expiration state and re-reads the chain plan each cycle. It also defines five hooks the loop-coupled deliverables plug into, each with a no-op default so the loop ships on its own:
  1. a startup hook, called once before the first cycle,
  2. a per-tick hook, handed every minute the loop sees, session or not,
  3. a per-slot close-tag hook, asked what tag the minute carries,
  4. a cycle-outcome observer, handed each cycle's result,
  5. a skipped-slot hook, handed the capture slots the loop missed after an overrun.

  The per-tick hook was added after this entry was written, for D14's power assertion and
  D13's idle heartbeat. Both need a minute the loop is awake for rather than a minute it
  captures on, which is why neither could ride the cycle-outcome hook.
- **D10** startup gap-marking. Gap-marking writes an explicit marker for a missed minute, so a gap is recorded rather than silently absent. One `GapMarker` serves both of D9's hooks, so a restart and a live overrun leave the same kind of record. They differ only in the reason they stamp: `daemon_dead` for the minutes another incarnation never reached, `slot_overrun` for the ones a living loop slept through. The anchor counts marker rows as well as data rows, so a daemon restarted repeatedly under `KeepAlive` never marks a minute twice. Four things it builds that nothing had:
  1. `journal.last_recorded_slot`, the newest recorded minute for a surface, ticker, and day. `latest_expirations` could not answer this: it returns expiration dates, it is chains-only, and it filters to data rows.
  2. `journal.gap_rows`, one batch covering many missed minutes. A dark session is 406 slots, and `gap_batch` builds one row per call.
  3. `SessionClock.phase_at`, the phase of any slot rather than the current one, so a marker for a post-equity-close minute carries the phase a captured row would have.
  4. `session.missed_slots`, D9's own day-by-day walk moved beside `skipped_slots` so both hooks share one enumerator and `lake.gap` needs no import of `lake.daemon`.
- **[#88](https://github.com/l3a0/marketlake/issues/88).** Built. `manifest.backup_scrub` walks the rsync target and checks it against the lake's manifest rather than the copy of that manifest riding on the backup, and the Sunday job runs it beside `manifest.scrub`. The copy's own manifest is read for its length alone, which says how far the last sync got, so a partition sealed since then reads as pending rather than as loss. `--checksum` left `RsyncBackup.sync` in the same change, because the scrub now notices the bit rot the flag was standing in for and names the file instead of copying over it.
- **The close+15 compaction dispatch.** Built. The daemon dispatches the close+15 job
  through D11's `session.SessionDispatch`, on the same tick hook the close+5 guard rides,
  so the machine seals and backs up its own day instead of waiting for a hand-run
  `python -m lake.compact`. That entry stays for the catch-up and the run under an
  operator's eye, and the lake-root lock is what keeps the two from racing.

  **The job runs in its own process, not on the loop thread.** The design gives compaction
  and its backup roughly 25 minutes, and it gives the dead-man a 5-minute grace on an idle
  heartbeat that rides this same per-minute hook. Those two cannot both hold on one thread.
  A compaction outrunning the grace would page "capture down" about a healthy daemon, which
  is the one page reserved for a daemon that is actually gone. A child process keeps the
  loop ticking through it, keeps an `rsync` and a ticker-day of Parquet out of the resident
  daemon's heap, and cannot take capture down by failing. Serialisation is already the
  lake-root lock's job, and the manifest protocol wrote that lock for exactly this shape:
  the scheduled run, a sleep-missed catch-up and a hand-invoked one contending for one
  lake. This adds a fourth caller rather than a new mechanism. `CompactionRunner` is the
  seam, mirroring the `AssertionRunner` the loop already spawns `caffeinate` through, and
  it is required rather than defaulted, because a caller who forgot it would run a real
  compaction against whatever config the daemon was handed.

  **A thread was considered and rejected.** It would keep the loop ticking too, and it is
  the smaller diff. It shares the daemon's heap, so an allocation failure or a native fault
  inside compaction reaches capture, and the memory a ticker-day's merge takes stays in a
  resident process rather than returning to the OS. It would also be a genuinely new
  sharing model in a daemon the design describes as one loop, where a child process is the
  model the lock already assumes.

  Two rules bound when the dispatch may fire.

  1. **One tick past its moment.** Sealing is the one act here that cannot be taken back,
     so it follows every writer that can still add a row to the day. Startup marking runs
     from `on_start`, before the first tick. The close+5 guard runs from the tick hook,
     already wrapped inside this one. The third decides it: `run_loop` calls `on_skipped`
     *after* `on_tick`, so a compaction dispatched from the tick the daemon woke on would
     seal the day a minute before the loop wrote the markers it owed for it, and the next
     run would delete them as debris. A lid closed at 16:10 and opened at 17:00 is that
     case, and it is an ordinary laptop day. The price is one minute against a job the
     design schedules fifteen minutes past the close.
  2. **Never inside a capture window.** A stall that begins just past one day's close+15
     and ends inside the next session leaves the dispatch owed on a minute the loop is
     capturing, and starting it there would put a seal and a full-lake `rsync` in front of
     a live fetch. The day it skips is swept by the next run outside the window, because
     the job already walks every date under `journal/`.

  **A day with no session still owes the job.** `SessionDispatch` refuses a non-session day,
  because close+15 does not exist without a close, and the design has compaction run at its
  regular wall-clock time on a non-session weekday instead. A job that correctly no-ops
  still pings, and silence always means broken rather than idle, so skipping holidays would
  take the `compaction` check down nine or ten times a year on a daemon doing exactly the
  right thing. The dispatcher takes an opt-in `fallback` for it, default off, so the close+5
  guard's holiday refusal is unchanged. `control_plane.COMPACTION_RUN` is the 16:30 constant,
  beside `VENDOR_SWEEP`'s 18:30, and a weekend owes nothing because the check expects a ping
  on weekdays only.

  One thing this change had to fix beside itself. The close+5 guard read a ticker-day's
  close tags from the segment directory alone, and compaction unlinks that directory once
  the partition is manifested. Before the daemon sealed its own day the two never
  overlapped. Now a restart after the evening's seal made the guard read an empty directory,
  call a captured close unobserved, and write a false marker that the next run deleted as
  debris. Gap-marking already skipped a sealed date for this exact reason. The guard now
  does too.

- **[#95](https://github.com/l3a0/marketlake/issues/95), [#113](https://github.com/l3a0/marketlake/issues/113) and [#114](https://github.com/l3a0/marketlake/issues/114).** Three residual gaps in
  the daemon's production wiring. `test_daemon_wiring.py` covers the hook bindings
  `run_loop_from_config` builds, and deleting any one of them fails a test. What no test
  covers is narrower than a binding. This entry listed seven. Four have closed since, each
  to a different change rather than to one:

  - the close-guard layer's `on_tick` pass-through, to D15's idle stamp,
  - the guard's dispatch moment, to the wiring tests themselves,
  - the gap marker's security master, to the close+15 dispatch,
  - the dead-man's `any` over a cycle's segments, to one cycle carrying a data segment
    beside a gap and one carrying no segment at all.

  Each of the three below was re-confirmed by mutating `src/lake/daemon.py` and running
  the whole suite, which stays green:

  1. the `session_phase` the loop forwards into the production cycle runner, and the fact
     that no test carries either it or the close tag into a journaled row through the
     production entry, which is [#95](https://github.com/l3a0/marketlake/issues/95). The `close_tag` half of the forward is
     covered, by `test_the_daemon_answers_the_close_tag_hook_from_the_calendar`,
  2. the caller's `on_start` and `on_skipped` pass-throughs in the gap-marker layer, which
     is [#113](https://github.com/l3a0/marketlake/issues/113),
  3. the watchdog instance the skipped-slot hook charges, which no test ties to the one the
     cycle observer feeds, which is [#114](https://github.com/l3a0/marketlake/issues/114).
- **D11** close tags and the close+5 guard. Close+5 is the five-minute window after the option close, the last moment an option-close fetch may land. It plugs into D9's close-tag hook, and it builds the session-relative dispatcher the design calls for. Everything session-relative runs from inside the daemon, because launchd's calendar intervals are fixed wall-clock and cannot express a close-relative time. `SessionDispatch` fires one job once per session day at a moment the calendar decides, including on a daemon that starts after that moment has passed. The close+15 compaction dispatch binds to the same seam, one tick later than its own moment, for the reason the entry above gives. Two rules are worth stating where both writers can see them:
  1. The guard's fill triggers on missing marks, not a missing cycle. A chain that failed at the option close leaves a tagged gap row holding nothing a reader can price against, and a close+5 refetch is exactly what rescues it.
  2. On a post-close restart the guard runs before startup gap-marking, so the two close minutes it owns are already recorded when D10's marker walks the day.
- **D11's fill.** The producer `daemon._close_guard` hands the guard. It fetches by the chain
  chunk plan through `capture.fetch_chain` and lands the result through
  `capture.journal_snapshot`, so the fill and a loop cycle share one code path rather than two.
  A whole chain in one request trips the gateway body limit, and the biggest chains are both
  the likeliest to trip it and the likeliest to need rescuing, which is why the fill reuses the
  plan rather than firing once. The landed rows carry the close slot in `snap_ts` and the fetch
  minute in `fetch_ts`. A fill that captured nothing writes no row. The day already holds the
  gap row from the cycle that failed at the close, and a second row for that minute would
  double-count it in every per-slot completeness read. A window that answers 200 with an
  empty chain counts as captured nothing, because Schwab reports some faults in the body
  rather than in the status. A fill that landed but gave up a window is named in the
  guard's outcome, since the membership comparison cannot see a window that failed both
  intraday and at close+5.
- **The chain fetch's two fail-open branches.** Both predate the close+5 fill, which carried
  them across unchanged.
  1. A reassembled body the row builder rejects fails open to a whole-chain gap row carrying
     that failure's own class. Letting it propagate would leave the cycle runner, leave the
     daemon loop, and exit the process, and the `KeepAlive` successor would reach the same
     minute and do it again, so one malformed payload costs every capture minute until
     someone notices it. This branch is narrower than it was. A known field whose value its
     column refuses is now routed into `extra` and the cycle lands, so what still reaches
     this fail-open is a value with no key in the overflow to land under. The design doc's
     schema-policy section carries the rule and the columns it covers.
  2. A window body the merge cannot read is recorded once under `chain_schema_drift`, a class
     of its own, and never split. The other windows still land, so the loss is that one
     window. Two shapes reach it.
     1. An expiration whose value is not a strike map, which has no `items` to walk.
     2. A strike whose value is not a contract list, which the merge refuses by type.

     The second is a type test rather than an attempt, because a string and a Mapping are both
     iterable. `list.extend` used to consume one a character or a key at a time, and the row
     builder met those where contract dicts belong, which cost the whole chain. That is
     [#305](https://github.com/l3a0/marketlake/issues/305).
     **Considered and rejected: splitting a window the merge could not read**, which is what
     the fetch did until #305. A date-keyed split does narrow the damage, because a drifted
     expiration sits on one date, and the depth bound is where the narrowing stops. Against the
     default plan at a bound of 4 the narrowest sub-range a split reaches is 1 day inside the
     ten-day window, 1 inside the twenty-one-day, 3 inside the sixty-day, and 17 inside the
     two-hundred-and-seventy-five-day one, so the far term gave up a fortnight rather than a
     day. What the split spends to buy that is the measurement that retired it. A payload
     change reaching every expiration costs 113 requests for one ticker-minute against the 5 a
     healthy one makes: 19 for the ten-day window, which bottoms out on single days before the
     bound and so never walks a full tree, 31 for each of the other three, and 1 for the open
     tail. The minute goes first and one ticker is enough, since a healthy SPY chain fetch
     already takes about nine seconds for its five windows. The 120 req/min ceiling goes next,
     at two option tickers. A cycle that overruns fires no cycle in the next minute and charges
     every watched surface, quotes included. The entry this replaces rejected two proposals,
     stopping the split once both halves have failed and bounding drift lower than size,
     because either bought a smaller number in a case the vendor has never produced. What
     answers that is the ceiling rather than the size: past it the number stops being smaller
     or larger and becomes a different failure, one reaching the quote surface a chain fetch
     cannot otherwise touch. Giving up the window is neither proposal, because it leaves no
     split for a second rule to govern. The depth bound still caps a too-big window's split and
     is deliberately the only cap there. Filing drift under the size class would send a reader
     to the chunk plan for a problem no chunk plan fixes. The class is named to read as drift, so a reader sweeping the gap classes finds
     it under one string, and it matches the reason the segment readers are to carry for the
     same signal in [#104](https://github.com/l3a0/marketlake/issues/104), which is not built
     here. The parser's schema-drift page shipped in
     [#197](https://github.com/l3a0/marketlake/issues/197) reads a different signal, a known
     field's name sitting in `extra` on a chain the parser could read, so it subscribes to no
     gap class. Who hears about a given-up window depends on what else landed. A chain whose
     every window drifted is a whole-chain gap, and the watchdog is what speaks for a ticker
     that stopped producing data. A chain that lost one window lands as data carrying that
     window's absence marker, which resets the watchdog rather than tripping it, so the marker
     rows are what a reader has.
     The merge itself reads a whole body into scratch maps and copies them into the
     reassembly maps only on success. So a window given up carries no data rows beside the
     absence marker saying it was never collected, which is the double-record the markers
     exist to prevent.
- **D11's membership marker.** Every expiration the intraday chain carried and the close+5
  fill did not becomes one row under `option_close_series_absent`, written by the guard
  into its own segment beside the fill's. Only series whose date window was fetched
  successfully get that reason. One inside a window the fetch gave up was missed rather
  than withdrawn, and the fill already marked it with that window's own class, so the
  guard subtracts those before it writes. That subtraction is what the fill's widened
  return exists for: it hands back what it captured, the windows it lost, and the
  representative error class, because a bare expiration list cannot tell the two
  populations apart. The marker's `session_phase` is read off the calendar rather than
  hardcoded, which is why the equity-close marker carries null (16:00 is the last minute
  of `open`) and this one carries `post_equity_close`. The findings the guard produces
  still reach the daemon's stderr through `_report_guard`, so launchd captures them to a
  log file. A log file is a worse home than a page or a panel, and that is a separate
  question from this entry.
- **[#118](https://github.com/l3a0/marketlake/issues/118).** The Today strip and the Now
  panel report every error class a slot carried, rather than the one its rows named most
  often. Picking the most common reason counted rows, and absence markers are written per
  series rather than per cycle, so they outnumbered the row recording why the cycle failed
  and the vote went to the benign reason. A 16:15 that failed `http_500` read as
  `chain_chunk_failed` once the fill gave up a window naming five series, which was
  reachable without the membership marker at all. The marker widened the count rather than
  creating the hole. No figure was wrong. The one reason string an operator reads was.
  Reporting the set discards nothing. How many reasons a slot can hold follows from how
  many of its windows failed, and the chunk plan's window count and the split depth bound
  that, so the list stays a handful of strings. `error_class` on a slot and
  `last_error_class` on a Now row are lists in both payloads, ordered by name. Each
  payload still carries its count beside the list, derived from that list rather than
  counted separately. The design doc's dashboard section carries why the two alternatives
  were cut.
- **D12** compaction and backup, plus the nightly window re-tune. Compaction merges a day's segments into one sealed partition. The re-tune runs after it. The job groups the day's rows by `window_start` and `window_end`, compares each window's contract count to the body limit, and rewrites `chain_plan.json` when the profile drifts.
- **D12's exclusion list.** The design's *Backup, defined* names the sync root as `lake/`
  only, "with an explicit exclusion list". `runner.BACKUP_EXCLUSIONS` is now that list,
  and it holds two entries. `*.tmp-*` is the temp file an atomic write leaves behind,
  which carries no manifest entry and would plant an orphan for the backup-copy scrub
  above. `.config/marketlake/` is the directory holding the token and `config.yaml`'s four
  secrets. That directory is outside the sync root today, so the pattern matches nothing
  and costs nothing. It exists so the rule holds by exclusion rather than by luck, and
  keeps holding the first time the sync root widens. Both are derived from constants in
  `paths`, because `compact` spelled the temp name twice before and a second spelling
  would put a temp file outside the exclusion with nothing to say so.
- **D13** watchdog and alerting. One counter per ticker and surface. A durable data cycle resets it, a gap row does not, and three consecutive session minutes page once. Counters start at zero on every restart, never rebuilt from the journal, so a restart never pages for the downtime that preceded it. It observes both D9's cycle-outcome hook and its skipped-slot hook, because the loop runs no cycle for a slot it slept through and those are the minutes the daemon was worst off. Three collapses keep a page storm from replacing a diagnosis:
  1. Every quotes ticker rides one batched request, so all of them failing together is one page naming the sampler.
  2. A cycle where every surface failed with the same known class pages that cause instead. A dead refresh token gaps chains and quotes for every ticker at once, and the design expects one every seven days.
  3. A slot the loop slept through gaps every watched surface at once, so one overrun that trips the threshold is one page rather than one per surface.

  A page that never reached the phone is written to a dated directory under `reports/`, one write-once file each, so the count on the Now panel has a source. It also ships the 09:35 says-closed-but-open probe, its page, its plist through D14's renderer, and the `capture` dead-man feed with its idle heartbeats.
- **[#265](https://github.com/l3a0/marketlake/issues/265).** The half of the parser's schema-drift page a vanished field leaves no trace of. A retype shows as a known field's name sitting in `extra`, the signature [#129](https://github.com/l3a0/marketlake/issues/129) narrowed the class to, and a field that stops arriving shows as nothing at all. So it is a second detection on the producer [#197](https://github.com/l3a0/marketlake/issues/197) builds, reading the same built batch and the same cadence state, which is why it sequences after that producer rather than beside it.
- **D14** laptop control plane. `render --out DIR` writes every plist and setup file to a directory and prints the install commands. It refuses a system directory, and nothing here runs `sudo`, a `pmset` write, `launchctl bootstrap`, or a `tmutil` write. What does run is read-only and needs no root: `launchctl print` and `pmset -g assertions` from the self-check, `pmset -g sched` and `tmutil isexcluded` from the Sunday job. The token path comes from one rule, so the daemon that rewrites it, the Sunday job that asserts coverage over it, and the exclusion that protects it cannot name different files. `RunAtLoad` is on for the two residents and the self-check. It is off for the Sunday job, which would otherwise scrub the whole lake at every boot. Beside the sudoers drop-in it renders five LaunchDaemons, and it prints the Time Machine exclusion as an install step rather than rendering it:
  1. the capture daemon, resident under `KeepAlive`,
  2. the query service, resident the same way,
  3. the weekday pre-open self-check, on a calendar interval,
  4. the 09:35 calendar probe, the says-closed-but-open guard,
  5. the Sunday maintenance job, on its own.

  `render` also writes an executable `install.sh`, so the privileged half is one command
  the operator runs rather than seventeen lines pasted by hand. The **by-hand paste is
  considered and rejected** as the only path. It was chosen so the operator saw each root
  command before running it, and so the `visudo` gate was a natural place to stop. The
  paste is itself a failure mode, and the way it fails is the argument. A skipped line
  surfaces on a different check, at a different time, naming a different cause. Skip
  step 2 and nothing breaks that day. The drop-in grants only the two `pmset` writes,
  which no job needs until the Friday sweep tries to set the Sunday one-shot. The
  `sunday` check then pages the following Sunday at 23:30, up to a week after the
  mistake, naming the canary rather than the install.

  The script owes three things, which are the price of dropping the paste:

  1. It stops at the first failure, so a `visudo` that rejects the drop-in never reaches
     the `install` that would place it.
  2. It echoes each privileged command before running it, so the transcript shows what
     ran as root.
  3. It ends on `launchctl print`, so the operator reads whether the daemon came up.

  The cost is named, and it is smaller than it looks. Of the seventeen lines in steps 1
  to 5, thirteen call `sudo` and four never do. `sudo` also keeps its timestamp per
  terminal for `timestamp_timeout` minutes, so a paste already answers one prompt for
  a run of them. What the script actually costs is the reading. The operator no longer
  sees each root command before it runs. The renderer still executes nothing. It writes
  `install.sh` and never runs it, and `--out` still refuses a system directory, so the
  root-owned copy remains the operator's own act.

  `render` writes one more script beside it, `uninstall.sh`. The
  uninstall runs the install backwards. The install writes the plists (step 1), the
  sudoers drop-in (step 2), the weekday wake (step 3), the Time Machine exclusion
  (step 4), then bootstraps the labels (step 5). The uninstall runs 5, 3, 2, 1. Putting
  the bootout first is the load-bearing half, because a plist deleted under a loaded
  label leaves launchd holding a definition whose file is gone. A label that is not
  loaded is skipped rather than treated as a failure, so the uninstall converges from a
  half-finished install as well as a whole one.

  Three things it leaves:

  1. The lake. Deleting captured data is not part of undoing an install.
  2. The config directory and, with it, install step 4's Time Machine exclusion. The
     directory holds the token, `config.yaml`, and `tickers.yaml`, all of which survive
     an uninstall, so the guard over them survives too. Symmetry with the install is the
     wrong principle for a protection over data that outlives the install. Lifting the
     exclusion would put the token and `config.yaml`'s four secrets on the next hourly
     backup, and a backup that already ran cannot be un-run by re-adding the exclusion.
  3. The Sunday one-shot wake. `pmset schedule cancel` can take a single event, but only
     by naming the exact date and time it was set for. Nothing here knows which Sunday
     is pending without parsing `pmset -g sched`, which is more machinery than one wake
     is worth. The one-shot fires once and is then gone.

  Cancelling the weekday wake is the one place the uninstall reaches past what the
  install placed, and the cost is named rather than denied. macOS holds one *pair* of
  repeating power events, a power-on and a power-off, and `pmset repeat cancel` clears
  the pair. No command cancels half of it. The design doc's `pmset` table already
  records that second slot: it is why the sudoers rule spells its argument out instead
  of wildcarding it, since the wildcard would have granted a password-free repeating
  shutdown. So a repeating sleep the operator set elsewhere goes with the 08:25 wake.
  The script prints `pmset -g sched` before the cancel as well as after, so the
  transcript carries what to re-set by hand. Leaving the wake instead was rejected: an
  uninstalled machine that still wakes at 08:25 every weekday is the install's most
  visible residue, and the operator ran an uninstall to be rid of it.

  Reinstalling after a re-render is those two scripts, in order, and nothing else:

  ```bash
  ./uninstall.sh && ./install.sh
  ```

  Both headers carry that line, because `render` writes the scripts to a directory and
  prints the install text to stdout. The directory is the only surface an operator comes
  back to. The `&&` is load-bearing rather than punctuation. An uninstall that cannot
  finish has to leave the install unrun, instead of layering a new install over a broken
  one, and a `;` would run it anyway.

  Two things are pinned as **considered and rejected** here.

  1. **The in-place plist swap.** An earlier reinstall overwrote the plists, booted the
     labels out and back in, and left install steps 2, 3 and 4 to the operator. It named
     that as a limit rather than a property, and its header handed over a `sudo diff` of
     the drop-in to run after any re-render. The gap was documented, not denied. What
     makes documenting it insufficient is the shape of the case that bites. Re-tuning
     either wake constant rewrites the drop-in while leaving every plist byte-identical,
     so the operator who checks the plists sees nothing to do and skips the diff that
     mattered. Running both halves closes the gap instead of describing it.
  2. **A rendered `reinstall.sh`.** Once the swap was cut, the file held six lines that
     called the other two scripts, and `./uninstall.sh && ./install.sh` is behaviourally
     identical: same command log, same exit codes, same short-circuit. What decided it
     was drift, not tidiness. Within one commit of being reduced to a composition, its
     header restated the uninstall's counted set of three survivals as two and dropped
     the Sunday one-shot. A file whose stated purpose was to remove a second description
     of an install had produced one. The three facts that lived only in it moved into the
     two headers that remain: the composed command, why the separator is `&&`, and that
     the install half re-sets only the 08:25 wake, so it does not put back the power-off
     event the uninstall's step 2 took. Re-adding the file is a constant, one `render_all`
     line and a golden, if a reinstall ever earns a step of its own.

  `render` also writes `restart.sh`, which restarts a resident job so it picks up new
  code. Two of the five jobs can go stale, and the reason is the shape of the job rather
  than anything about the code. The daemon and the dashboard are resident: launchd starts
  each once and `KeepAlive` relaunches it if it exits, so each holds the Python it
  imported at start. The venv is an editable install whose path entry is the absolute
  `src` directory, so editing that tree changes what a *new* process imports and nothing
  about one already running. The self-check, the calendar probe and the Sunday job exec
  fresh on every fire, so they always run current code. The script derives that pair from
  `keep_alive` rather than listing it.

  `launchctl kickstart -k` runs the service immediately whatever its launch conditions
  say, killing the running instance first if there is one. That is right when the code
  changed and the plist did not, and wrong after a re-render that changed a plist, because
  the old definition is what gets run. The install text already covered the second case.

  The first case was not untooled. `./uninstall.sh && ./install.sh` boots every label out
  and back in, so it does restart both residents. It is the wrong shape for the job: it
  takes the whole control plane off the machine and puts it back, cancelling the firmware
  wake and re-validating the sudoers drop-in on the way, to achieve a process restart.

  Two launchd states read the same through a pid and must not be confused. `launchctl
  print` exits 0 for any label in the domain and 113 for one that is not, while the `pid`
  line appears only while a process is actually running. A label loaded but between
  processes therefore prints no pid, exactly like one never installed. Telling the
  operator to run the install there would be wrong twice over: the diagnosis is false, and
  `install.sh` bootstraps every label under `set -e`, which fails on a label already in
  the domain. So loadedness comes from the exit code and running-ness from the pid line,
  asked separately. That state is not exotic. It is where a resident crash-looping on bad
  code sits, which is the case a restart tool most needs to handle.

  A new pid is not yet a working service either. A resident that dies on import gets a
  fresh pid within seconds, so "the pid changed" is satisfied by precisely the failure a
  restart after a code change is most likely to cause. The script waits and requires the
  new pid to still be there, and points at the job's error log when it is not. Reporting
  success on a crash-looping job would make the read-back worse than no read-back.

  Two things are pinned as **considered and rejected** for the restart.

  1. **A bootout-and-bootstrap restart.** It would also pick up a changed plist, which
     makes it look like the more general tool. It is the more dangerous one. Booting a
     label out drops it from the domain, so a failure between the bootout and the
     bootstrap leaves the service down rather than merely unrestarted. A `kickstart -k`
     cannot reach that state, because launchd holds the definition throughout. Picking up
     a changed plist is the install's job.
  2. **Defaulting to both residents.** Restarting the dashboard costs its open
     connections. Restarting the daemon costs the in-flight cycle and its `caffeinate`
     assertion until it is back. A bare `./restart.sh` must not be the command that takes
     capture down, so it restarts the dashboard alone and the daemon has to be named.

  Unlike the cut `reinstall.sh`, this one is not a wrapper around a single command. It
  reports the working tree the services will adopt, refuses a job that is not loaded, and
  reads the pid back to prove the process actually changed. A kickstart that silently did
  nothing is the failure worth catching, and a bare command cannot catch it.

  The pasteable `INSTALL.txt` keeps the step-by-step procedure for operators who want it.
  It now leads with the composed command and carries the `sudo diff` line, and it points
  at `uninstall.sh`'s header for what an uninstall leaves rather than restating the list.

  The Sunday job's two outward seams are supplied, so the `sunday` command line no longer
  falls back on a canary that passes without calling anything or a reminder that reaches
  a log file and never a phone. `token_canary` is the throwaway authenticated call. It
  rebuilds the vendor from `token.json` inside every attempt and quotes one symbol, which
  is what lets the 21:00 retry see a re-login done at 20:40. Any failure answers `False`,
  because a call that did not come back has proved nothing, and the half-hour retry to
  23:00 is what absorbs a transient outage rather than paging on it. The seam keeps no
  default at all now. `sunday_maintenance` and `sunday_run` require a canary, so no later
  caller can leave one out and be told the weekend passed.

  `reminder_publisher` is the re-auth reminder's delivery. It pushes through the alert
  publisher, so an unreachable ntfy costs a log line and a dated file under `reports/`
  instead of taking the scrub, the alarm read-back and the check's own ping with it.

  One alert rule moved with them. Only a page carries the `rotating_light` tag now,
  because the design gives the emoji to a page alone and the reminder is the first
  message at the reminder tier to go through the transport. The tag follows the priority
  rather than becoming a second field a producer could set wrong.
- **D15** query service with the Now and Today panels. The query service is the read-only localhost dashboard.
- **D15's writers.** The Now panel reads three stamped facts no captured row can carry: the token's mint stamp, the last dead-man ping, and the count of pages that failed to send. Every one of them is now filled from under `lake_root`, because the dashboard never opens `~/.config`. `lake.metadata` owns the stamp at `journal/metadata.json`, which sits inside the reverse scrub's journal exclusion and outside the date directories compaction prunes. Four writers fill it:
  1. The capture cycle stamps the token's mint time, off the vendor it fetched with, and the cycle's roster as the surfaces each ticker is captured on. That roster stamp is what lets the panel show a ticker that journaled nothing as failing rather than dropping it.
  2. The daemon stamps the same two facts on every minute off the capture window, reading the mint from `token.json` because no client exists there. That is the path the Sunday re-auth reaches the panel by on the night it happens.
  3. The dead-man records each landed ping, and only a landed one.
  4. The tick hook that holds the power assertion stamps the pid of the `caffeinate` it is holding, and clears it once the window closes. The Now panel does not show that one. It is there for the 08:30 self-check, which runs as its own process and can tell the daemon's holder from anyone else's only by matching the pid.

  The page count needed no writer. `alert.undelivered` already counted the pages that never reached the phone, and the panel now reads it for the Eastern day the publisher files them under. The ages and the countdown are arithmetic over those stamps, the countdown against `control_plane.sunday_canary_due`, which keeps the ritual's moment beside the ritual. The dead-man's starvation verdict is arithmetic too, over its ping and the last minute the check was owed one.

Every issue-linked entry above belongs to slice 2, and none is deferred. The test is whether an
entry needs something a later slice introduces. None does. Slice 3 adds vendor-fetch surfaces,
which is D16's bars and actions. The entries above that touch the vendor reuse `SchwabVendor`,
which already ships. Slice 4 is pure derivation over sealed partitions and fetches nothing.
Slice 5 adds the validation battery and the History and Lake panels, so it owns neither the
Now panel's fields nor a scrub that already runs in the Sunday job. Two items in the
integration roster below are the only slice-2-era work that genuinely waits on slice 3, and
they are tests rather than deliverables.

Slice 2 builds in two waves. D9 comes first and defines the hooks. D14 and D15 do not touch the loop, so they build in parallel with D9. D12 built in parallel too, and its job later came back to the loop: the close+15 dispatch above is what wires it there. D10, D11, and D13 plug into D9's hooks, so they follow it, in parallel with each other.

### Slice 3, vendor fetch

The deliverable entries below are links. Each issue is the source of truth for its own scope, per the directive in `CLAUDE.md`. What stays here is the slicing rule, the build order, and each slice's test surface.

Slice 3 adds the vendor-fetch surfaces. Recorded vendor payloads are the test surface for the pieces that fetch. The pieces that derive from sealed partitions test against a fixture lake, since nothing in them reaches a vendor.

- **D16** bars, actions, and the validation that gates them. Scope in [#134](https://github.com/l3a0/marketlake/issues/134).

### Slice 4, the read layer

Slice 4 is pure derivation over sealed partitions. It fetches nothing. Its test surface is a fixture lake and a throwaway `config.yaml` naming one.

- **D17** loader API and adjusted views. Scope in [#135](https://github.com/l3a0/marketlake/issues/135).
- **D18** chains-to-bars join views. Scope in [#136](https://github.com/l3a0/marketlake/issues/136).
- **D19** the OI view. Scope in [#137](https://github.com/l3a0/marketlake/issues/137).

### Slice 5, validation and the full dashboard

Slice 5 adds the validation battery, the rest of the dashboard, and the quarantine sign-off tool.

- **D20** validation battery plus the History and Lake panels. Scope in [#138](https://github.com/l3a0/marketlake/issues/138).
- **D21** the quarantine sign-off tool, the flock-guarded CLI that resolves quarantines, placed beside the panel that surfaces them. Scope in [#139](https://github.com/l3a0/marketlake/issues/139).

Computed greeks stay deferred beyond the build, per the design doc.

## Test tiers

Every test sits in one of four tiers. The tier is set by the widest boundary the test must cross, not by what it is about.

| Tier | Scope | Target | Runs on |
| --- | --- | --- | --- |
| Unit | One module, every seam faked. Decided from values alone. | 320 tests, under 10 seconds | every save |
| Component | One subsystem across exactly one real boundary. Real files, real DuckDB, or real processes contending on a lock. Clock and vendor stay fake. | 60 tests, under 60 seconds | every commit |
| Integration | Two or more subsystems wired through real boundaries. | 14 named tests, two to four minutes | every push |
| Live | Needs the real vendor, the real OS scheduler, or real elapsed time. Deliberately not in CI. | 7 checks | by hand |

Two of these tiers get a named roster below. The 14 integration tests and the 7 live checks are each small and hand-picked, so every scenario is pinned by name. Unit and component are not rostered. Their counts are targets, filled per module and per subsystem as the build proceeds.

## The placement rule

One rule places every test. Apply it in order and stop at the first match.

1. Decided from values alone is unit.
2. Needs a real file, process, or query engine within one subsystem is component.
3. Needs two or more subsystems talking is integration.
4. Needs the real vendor, the real OS scheduler, or real wall-clock time to pass is not a test at all. It goes in the live lane.

## The 14 integration tests

1. Slice-1 cycle end to end.
2. Full simulated session.
3. Kill and restart mid-day. Slow.
4. Kill compaction mid-seal.
5. Early-close day.
6. Overnight death.
7. Fully dark session.
8. Holiday idle day.
9. Onboard mid-session.
10. Watchdog scenarios.
11. Auth death and recovery.
12. Nightly sweep chain.
13. Synthetic split replay.
14. Restore from backup.

Five of those have no test today. `tests/integration/` holds five files, three of them
roster items, and the rest of the roster is served at the component level, which is fine
for the ones that need no real process to die partway. Test 4 is the first test in any
tier to kill a running process, and the child it kills is
`tests/support/compaction_child.py`. The five split into two kinds.

All five are buildable now, and each covers a failure the unit and component suites cannot
reach:

1. 6, overnight death,
2. 7, fully dark session,
3. 12, the nightly sweep chain,
4. 13, synthetic split replay,
5. 14, restore from backup.

Test 13 joined that list when [#279](https://github.com/l3a0/marketlake/issues/279) shipped
the detector it replays against. The fixture builder can express a split now that
`tests/support/lake.py` carries `option_root`, the four deliverable columns, `mini` and
`is_chain_truncated`. [#353](https://github.com/l3a0/marketlake/issues/353) added `ssid`
beside them, which is what lets a fixture say *which* contract was re-symboled into which
rather than only that a root changed, so the replay can assert the master's mapping rows and
not just the ledger's entry. Test 12 joined the list when
[#281](https://github.com/l3a0/marketlake/issues/281) shipped the 18:30 job it replays, so
nothing on the roster is blocked on unbuilt work any more.

## The 7 live checks

These need the real world. They run by hand, off CI.

1. Record cassettes from one real Schwab call.
2. The day-one measurements.
3. The 08:25 wake and the 08:30 pre-open self-check.
4. The Friday-set Sunday one-shot, read back with `pmset -g sched`. pmset is the macOS power-scheduling tool.
5. The DST-weekend one-shot behavior. DST is the daylight-saving-time clock change.
6. A real restore from the SSD.
7. The Sunday canary coverage assertion. The Sunday canary is the weekend check that proves capture still works.

## When the alert channels get created

Each healthchecks.io check is created by hand, in the session that first makes its job ping, and never earlier. A check pages only after its first ping, so a row created ahead of its job is silent until then. Create it late anyway. A row with no producer reads exactly like the mistyped-slug failure the design calls silence. Creating it late has a cost now, and it is small and worth naming. Every live producer pages once when healthchecks refuses its ping, so a job that runs before its row exists sends one page saying so. That page is the feature working. Create the row and the next ping clears it. The design doc's monitoring table holds the slug for each one, and the ntfy table beside it holds every message shape and the integration settings. The ntfy topic itself is a D1 config key, hand-generated with the rest of `config.yaml`.

1. **D8**, `slice1-capture`, and the channel. Subscribe the phone to the topic from the clipboard, never from a printed string. Create the healthchecks.io project, its ntfy integration and its email integration with the design's settings, and the `slice1-capture` check named `Slice-1 capture`. Confirm ntfy and email both read on for it. Prove the chain before the first unattended run. Give the check a 2-minute period and a 1-minute grace, ping once, wait for `Slice-1 capture is DOWN` on the phone, ping again for `is UP`, then set the real envelope. A new check sends no up push on its first ping, so down is the first thing the phone can show. The phone is an iPhone, and ntfy documents priority behavior for Android only. So the same session confirms that the priority-5 push interrupts the locked screen. It also sets the ntfy app's pass through Focus, the iPhone's do-not-disturb modes, by hand in each Focus, and confirms it with the same push. Slice 1 shipped before this step was written, so any part of it still owed runs before D13.
2. **D12**, `compaction`. Create the check and confirm ntfy and email both read on for it.
   The producer exists now: the daemon dispatches the job at close+15 and the job pings
   after its backup, so this row is owed on the next weekday the daemon runs through one.
   Weekday, not session: the dispatch falls back to the regular wall-clock time on a
   holiday, so the check expects a ping every weekday and a holiday is not an exception to
   it. This row was the one that went missing, and it is what
   [#213](https://github.com/l3a0/marketlake/issues/213) came from. Until a row exists the
   ping goes to a slug healthchecks does not know, and the job now pages once to say so
   rather than reporting it on a log line nobody reads.
3. **D13**, `capture`, and the daemon's own pages. The per-cycle dead-man. Delete the `slice1-capture` row in the same session, because `capture` supersedes it. Ship every daemon page path through one publisher: auth death, sustained 429s, the watchdog, and the sampler collapse. The parser's schema-drift page carries the retype half, and the half a vanished field leaves no trace of is tracked in [#265](https://github.com/l3a0/marketlake/issues/265) above. The auth-gap reminder that #92 also carried is considered and rejected, in the design's auth-death bullet. Rehearse the topic rotation once, end to end. The 09:35 calendar probe ships here too, with its page and its `calendar-probe` check.
4. **D14**, `pre-open` and `sunday`, and the Sunday reminder. D14 renders the launchd jobs and the wake schedules those two checks watch. The Sunday job sends the re-auth reminder on its 20:00, 21:00, and 22:00 canary runs only, while the throwaway call or the coverage assertion still fails, reading the token's mint time from `token.json` itself.
5. **D16**, `eod-sweep`, and the nightly summary. `lake.sweep` ships the producer, so this row is owed on the next weekday its 18:30 job runs through. Create the check and confirm ntfy and email both read on for it. Until the row exists the ping goes to a slug healthchecks does not know, and the job pages once to say so, which is the refused-ping page working rather than a fault. That page is the one [#213](https://github.com/l3a0/marketlake/issues/213) could not wire, because no code pinged this slug when it shipped. The report channel is created here too: the sweep's own ping lands before the digest it sends. Scope in [#281](https://github.com/l3a0/marketlake/issues/281).
6. **D20**, the battery's pages, delayed feed and nightly schema drift.

## Discipline rules

Two rules keep the pyramid upright. The pyramid is the shape of a healthy suite: many fast unit tests, fewer component tests, a thin layer of integration tests.

1. A bug's regression test goes to the lowest tier that can express it. A bug decided from values alone gets a unit test, never an integration test.
2. A new page-class path earns one unit test and at most one integration scenario. A page-class path is a code path that can raise a page-now alert. This caps the cost of each new alert.
