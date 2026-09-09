# Marketlake — build plan

This plan sequences the build. The design doc at [design.md](design.md) is the source of truth. Read it first. This plan says what ships, in what order, and how each piece is tested. It does not restate the design.

Status: PLAN, 2026-08-26.

## Slicing rule

A slice is a deliverable that ships on its own. Three tests decide whether something qualifies.

1. It is testable with the market closed and the network off.
2. It stands alone as a shippable unit.
3. It does not delay the capture clock.

The capture clock is the daily job that records the market. Nothing in a later slice may push that job later or make it less reliable.

## Deliverables

The build is a sequence of deliverables, D0 through D21. Each is one focused unit of work. They group into the five slices from the design doc, plus the test harness that comes first.

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
- **Unowned.** The backup-copy scrub, and the `--checksum` drop that waits on it. `manifest.scrub` reads under `lake_root` only, so nothing verifies the backup target today. `control_plane`'s Sunday gap list already names it as gap 3 of 5. The two land together or in that order, because until the scrub exists `--checksum` in `RsyncBackup.sync` is the only thing that would notice the backup rotting. Dropping it first trades a deadline that fails in a few years for a verification hole that starts now. **Both stay in slice 2.** The scrub is a Sunday-job activity, the Sunday job is D14's, and `manifest.scrub` already exists and already runs there over `lake_root`. Reaching the backup target is a target and a parameter rather than new machinery, so slice 5's validation battery is the wrong home for it. The pairing is a sequencing rule inside slice 2, not a reason to defer either half out of it.
- **Unowned.** The daemon's in-loop close+15 compaction dispatch. `compact.compact` is reachable only from `python -m lake.compact`, and no job renders it. The design's rule that a catch-up compaction of an unsealed day is ordered after startup gap-marking is satisfied in-process today, because `run_loop` calls `on_start` before its first tick. Whoever builds the dispatch owns keeping it so. D11 built the seam it binds to, `session.SessionDispatch`, so what remains is the compaction job itself. **It stays in slice 2.** Every part it needs is already here: the dispatcher from D11, `compact.compact` from D12, and the loop from D9. It fetches nothing, so slice 3 would not help it, and it blocks the `compaction` check, which cannot be created before a producer exists.
- **Unowned.** Tests for four of the daemon's production hook bindings. The hooks
  themselves are held, and so is each observer in isolation. What is unheld is the wiring
  `run_loop_from_config` builds, which is the wiring the launchd job actually runs. Four
  are in that state:

  1. the per-tick hook feeding the `capture` dead-man's idle heartbeat,
  2. the cycle hook feeding the same dead-man's `captured` signal, which is what arms the
     check on the first durable cycle and holds the whole-daemon guarantee,
  3. the per-tick hook reaching the close+5 guard's `SessionDispatch.check`,
  4. the per-cycle chain-plan re-read, which is what makes a nightly plan rewrite take
     effect the next minute.

  Each was confirmed by deleting the binding and running the suite, which stays green at
  1135. The point is not that the bindings are wrong. It is that nothing would notice if
  they became wrong, and two of the four are the paths that carry a failure to the phone.

  Two more were on this list and are now held. The skipped-slot hook reaching `GapMarker`
  and the same hook reaching the watchdog both fall out of the roster tests in
  `tests/component/test_gap_marking.py`, which drive `run_loop_from_config` through a real
  overrun. Deleting either binding fails them.
- **Unowned.** The close+5 guard's roster snapshot. `daemon._close_guard` loads the roster
  once and hands the object to `CloseGuard`, which keeps it for the daemon's life. The
  guard does not ride a capture cycle, so it is in the position gap-marking and the
  watchdog counters were in before they were made to read `tickers.yaml` themselves. The
  same two directions apply. A ticker onboarded mid-session owes a `spot_close` and an
  `option_close` the frozen roster never checks, and a ticker retired mid-session can
  collect a `spot_close_unobserved` marker it no longer owes. `daemon._roster_reader`
  already exists and would fit. **It stays in slice 2**, beside the guard it belongs to.
  What it needs first is not code but a decision: the guard is a per-day check rather than
  a per-minute record of an owed minute, so "why does it read the roster at that moment"
  has to be answered before "is the frozen one wrong". The design doc names it as open in
  the Onboarding section rather than settling it.
- **D11** close tags and the close+5 guard. Close+5 is the five-minute window after the option close, the last moment an option-close fetch may land. It plugs into D9's close-tag hook, and it builds the session-relative dispatcher the design calls for. Everything session-relative runs from inside the daemon, because launchd's calendar intervals are fixed wall-clock and cannot express a close-relative time. `SessionDispatch` fires one job once per session day at a moment the calendar decides, including on a daemon that starts after that moment has passed. The close+15 compaction dispatch binds to the same seam when someone builds it. Two rules are worth stating where both writers can see them:
  1. The guard's fill triggers on missing marks, not a missing cycle. A chain that failed at the option close leaves a tagged gap row holding nothing a reader can price against, and a close+5 refetch is exactly what rescues it.
  2. On a post-close restart the guard runs before startup gap-marking, so the two close minutes it owns are already recorded when D10's marker walks the day.
- **Unowned.** The close+5 fill's producer. `CloseGuard` takes an injected `fill` and
  `daemon._close_guard` never passes one, so `self._fill` is `None` in production. The guard
  detects a missing `option_close`, appends a `no fill fetcher` line to its own outcome, and
  returns. Nothing is refetched and nothing is written. The reason constant
  `OPTION_CLOSE_SERIES_ABSENT` is defined and exported and never written by any code path, and
  `_marker` is called once in the file, on the `spot_close` side. So the close minute leaves
  no trace from this writer at all. The whole point of close+5 is that option quotes freeze at
  the option close, so a fetch inside that window still observes the closing marks. Until the
  producer exists, the window is observed and never used. It needs no vendor work that slice 2
  lacks: `SchwabVendor.get_chain` already ships and capture calls it every minute. What is
  missing is a caller at close+5 and a decision about what a failed fill records.
- **Unowned.** The membership guard's absent-marker rows. The guard counts missing expirations
  and writes no marker for them, so a series that was never offered and one that was missed
  read the same downstream. This one is ordered behind the fill above, because the comparison
  it marks against only exists once a fill has landed. The findings the guard does produce
  reach the daemon's stderr through `_report_guard`, so launchd captures them to a log file. A
  log file is a worse home than a page or a panel, and that is a separate question from this
  entry.
- **D12** compaction and backup, plus the nightly window re-tune. Compaction merges a day's segments into one sealed partition. The re-tune runs after it. The job groups the day's rows by `window_start` and `window_end`, compares each window's contract count to the body limit, and rewrites `chain_plan.json` when the profile drifts.
- **Unowned.** The backup's exclusion list. The design's *Backup, defined* names the sync
  root as `lake/` only, "with an explicit exclusion list". Nothing in `compact` carries
  one. The token file is out of the sync root today by construction rather than by
  exclusion, so the rule holds by accident and would stop holding the first time the sync
  root widens.
- **D13** watchdog and alerting. One counter per ticker and surface. A durable data cycle resets it, a gap row does not, and three consecutive session minutes page once. Counters start at zero on every restart, never rebuilt from the journal, so a restart never pages for the downtime that preceded it. It observes both D9's cycle-outcome hook and its skipped-slot hook, because the loop runs no cycle for a slot it slept through and those are the minutes the daemon was worst off. Three collapses keep a page storm from replacing a diagnosis:
  1. Every quotes ticker rides one batched request, so all of them failing together is one page naming the sampler.
  2. A cycle where every surface failed with the same known class pages that cause instead. A dead refresh token gaps chains and quotes for every ticker at once, and the design expects one every seven days.
  3. A page that never reached the phone is written to a dated directory under `reports/`, one write-once file each, so the count on the Now panel has a source.
  It also ships the 09:35 says-closed-but-open probe, its page, its plist through D14's renderer, and the `capture` dead-man feed with its idle heartbeats.
- **Unowned.** Four of D13's page paths. The auth-gap reminder, because the watchdog's cause page is once-on-transition, so a token dying at 09:31 pages once and is then silent for the session. The parser's schema-drift page. The undelivered-pages count on the Now panel, which has a source now but no reader. And `--test-push` on the onboarding command. The publisher exists, so each is a producer and a schedule rather than new machinery.
- **Unowned.** The install's arming step. `capture` arms only when it has been pinged once. Outside the capture window an idle heartbeat does that on its own, so the exposure is an install made inside the capture window whose cycles all fail. That leaves the whole-daemon guarantee inert while every job reports healthy. `pre-open` does not cover it, because it asserts the daemon is up rather than that capture works. The install text ends on `launchctl print`, which answers whether the daemon started rather than whether it is being watched. It should end by arming the row from healthchecks' `Ping Now`, after the bootstrap and never before. That is one press, it works when capture is broken, and it is what converts a silent failed install into a page inside the grace.
- **D14** laptop control plane. `render --out DIR` writes every plist and setup file to a directory and prints the install commands. It refuses a system directory, and nothing here runs `sudo`, a `pmset` write, `launchctl bootstrap`, or a `tmutil` write. What does run is read-only and needs no root: `launchctl print` from the self-check, `pmset -g sched` and `tmutil isexcluded` from the Sunday job. The token path comes from one rule, so the daemon that rewrites it, the Sunday job that asserts coverage over it, and the exclusion that protects it cannot name different files. `RunAtLoad` is on for the two residents and the self-check. It is off for the Sunday job, which would otherwise scrub the whole lake at every boot. Beside the sudoers drop-in it renders five LaunchDaemons, and it prints the Time Machine exclusion as an install step rather than rendering it:
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
- **Unowned.** The Sunday re-auth reminder's delivery. `sunday_run` takes a
  `reminder_sink` and only fires it when one is passed. The `python -m lake.control_plane
  sunday` entry that the launchd job runs passes none, so the reminder is printed to the
  job's log file and never pushed. That is the same shape as D11's missing fill: the seam
  is built and the producer is not. It carries no blame for the September 2026 expiry,
  because the Sunday job was not installed until three days after it, but it is what would
  have to work for the next one to be announced.
- **Unowned.** The Sunday canary's producer. `sunday_run` takes an injected `canary`, the
  throwaway authenticated call that proves capture still works over a weekend, and the
  `sunday` CLI passes none. The fallback is `_canary_pass_through`, which returns `True`
  without calling anything. So the canary passes every Sunday whatever the token's state.
  This is the third seam in slice 2 built and never supplied, after D11's fill and the
  reminder above, and it is the costliest of the three. The other two fail to act. This one
  reports success.
- **D15** query service with the Now and Today panels. The query service is the read-only localhost dashboard.
- **Unowned.** Five Now-panel fields that are hardcoded `None`, and the writers each one
  waits on:

  1. `token_minted_at`, which needs the refresh token's mint stamp journaled,
  2. `token_age_minutes`, computed from that same stamp,
  3. `token_sunday_countdown_minutes`, computed from it too,
  4. `dead_man_last_ping`, which needs the watchdog's last ping recorded where the
     dashboard can read it,
  5. `pages_failed_to_send`, which has a source and no reader, and is already booked above
     as one of D13's four page paths.

  One writer clears the first three. The panel itself renders. It answers none of the
  questions an operator opens it to ask.
- **Unowned.** The roster stamp the design puts in journal metadata. `lake_roster` says
  so itself: until the stamp exists it reads the ticker list off the lake's own directory
  layout. So a ticker that journaled nothing has no directory and is missing from the panel
  entirely, rather than shown as failing. A surface that journaled once and then died does
  appear, with a growing `minutes_since`. The gap is the first case, where the dashboard
  cannot distinguish a ticker that was never expected from one that was expected and
  produced nothing.

Every unowned entry above belongs to slice 2, and none is deferred. The test is whether an
entry needs something a later slice introduces. None does. Slice 3 adds vendor-fetch surfaces,
which is D16's bars and actions. The entries above that touch the vendor reuse `SchwabVendor`,
which already ships. Slice 4 is pure derivation over sealed partitions and fetches nothing.
Slice 5 adds the validation battery and the History and Lake panels, so it owns neither the
Now panel's fields nor a scrub that already runs in the Sunday job. Two items in the
integration roster below are the only slice-2-era work that genuinely waits on slice 3, and
they are tests rather than deliverables.

Slice 2 builds in two waves. D9 comes first and defines the hooks. D12, D14, and D15 do not touch the loop, so they build in parallel with D9. D10, D11, and D13 plug into D9's hooks, so they follow it, in parallel with each other.

### Slice 3, vendor fetch

Slice 3 adds the vendor-fetch surfaces. Its test surface is recorded vendor payloads.

- **D16** bars, actions, and the cross-check.

### Slice 4, the read layer

Slice 4 is pure derivation over sealed partitions. It fetches nothing. Its test surface is a fixture lake.

- **D17** loader API and adjusted views.
- **D18** chains-to-bars join views.
- **D19** the OI view.

### Slice 5, validation and the full dashboard

Slice 5 adds the validation battery, the rest of the dashboard, and the quarantine sign-off tool.

- **D20** validation battery plus the History and Lake panels.
- **D21** the quarantine sign-off tool. It is the flock-guarded CLI that resolves quarantines, placed beside the panel that surfaces them.

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
4. Kill compaction mid-seal. Slow.
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

Six of those have no test today. `tests/integration/` holds three files, and the rest of
the roster is served at the component level, which is fine for the ones that need no real
process to die partway. The six split into two kinds.

Four are buildable now, and each covers a failure the unit and component suites cannot
reach:

1. 4, kill compaction mid-seal,
2. 6, overnight death,
3. 7, fully dark session,
4. 14, restore from backup.

Two are blocked on work that does not exist yet, because their subject is slice 3's:

1. 12, nightly sweep chain,
2. 13, synthetic split replay.

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

Each healthchecks.io check is created by hand, in the session that first makes its job ping, and never earlier. A check pages only after its first ping, so a row created ahead of its job is silent until then. Create it late anyway. A row with no producer reads exactly like the mistyped-slug failure the design calls silence. The design doc's monitoring table holds the slug for each one, and the ntfy table beside it holds every message shape and the integration settings. The ntfy topic itself is a D1 config key, hand-generated with the rest of `config.yaml`.

1. **D8**, `slice1-capture`, and the channel. Subscribe the phone to the topic from the clipboard, never from a printed string. Create the healthchecks.io project, its ntfy integration and its email integration with the design's settings, and the `slice1-capture` check named `Slice-1 capture`. Confirm ntfy and email both read on for it. Prove the chain before the first unattended run. Give the check a 2-minute period and a 1-minute grace, ping once, wait for `Slice-1 capture is DOWN` on the phone, ping again for `is UP`, then set the real envelope. A new check sends no up push on its first ping, so down is the first thing the phone can show. The phone is an iPhone, and ntfy documents priority behavior for Android only. So the same session confirms that the priority-5 push interrupts the locked screen. It also sets the ntfy app's pass through Focus, the iPhone's do-not-disturb modes, by hand in each Focus, and confirms it with the same push. Slice 1 shipped before this step was written, so any part of it still owed runs before D13.
2. **D12**, `compaction`. Create the check and confirm ntfy and email both read on for it.
3. **D13**, `capture`, and the daemon's own pages. The per-cycle dead-man. Delete the `slice1-capture` row in the same session, because `capture` supersedes it. Ship every daemon page path through one publisher: auth death, sustained 429s, the watchdog, and the sampler collapse. The auth-gap reminder, the parser's schema-drift page, the undelivered-pages counter on the Now panel, and a `--test-push` on the onboarding command are not built yet and are pinned as unowned above. Rehearse the topic rotation once, end to end. The 09:35 calendar probe ships here too, with its page and its `calendar-probe` check.
4. **D14**, `pre-open` and `sunday`, and the Sunday reminder. D14 renders the launchd jobs and the wake schedules those two checks watch. The Sunday job sends the re-auth reminder on its 20:00, 21:00, and 22:00 canary runs only, while the throwaway call or the coverage assertion still fails, reading the token's mint time from `token.json` itself.
5. **D16**, `eod-sweep`, and the nightly summary. The vendor sweep writes the dated report file under `reports/` and sends its one-screen digest at priority 2 after its own ping lands, holiday no-ops included. Until D20 the quarantine count is zero and the History panel that renders the file does not exist yet, so the file is read by hand.
6. **D20**, the battery's pages, delayed feed and nightly schema drift.

## Discipline rules

Two rules keep the pyramid upright. The pyramid is the shape of a healthy suite: many fast unit tests, fewer component tests, a thin layer of integration tests.

1. A bug's regression test goes to the lowest tier that can express it. A bug decided from values alone gets a unit test, never an integration test.
2. A new page-class path earns one unit test and at most one integration scenario. A page-class path is a code path that can raise a page-now alert. This caps the cost of each new alert.
