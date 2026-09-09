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

- **D9** daemon loop. It fires the capture cycle at each session minute and idles otherwise. It holds no expiration state and re-reads the chain plan each cycle. It also defines four hooks the loop-coupled deliverables plug into, each with a no-op default so the loop ships on its own:
  1. a startup hook, called once before the first cycle,
  2. a per-slot close-tag hook, asked what tag the minute carries,
  3. a cycle-outcome observer, handed each cycle's result,
  4. a skipped-slot hook, handed the capture slots the loop missed after an overrun.
- **D10** startup gap-marking. Gap-marking writes an explicit marker for a missed minute, so a gap is recorded rather than silently absent. One `GapMarker` serves both of D9's hooks, so a restart and a live overrun leave the same kind of record. They differ only in the reason they stamp: `daemon_dead` for the minutes another incarnation never reached, `slot_overrun` for the ones a living loop slept through. The anchor counts marker rows as well as data rows, so a daemon restarted repeatedly under `KeepAlive` never marks a minute twice. Four things it builds that nothing had:
  1. `journal.last_recorded_slot`, the newest recorded minute for a surface, ticker, and day. `latest_expirations` could not answer this: it returns expiration dates, it is chains-only, and it filters to data rows.
  2. `journal.gap_rows`, one batch covering many missed minutes. A dark session is 406 slots, and `gap_batch` builds one row per call.
  3. `SessionClock.phase_at`, the phase of any slot rather than the current one, so a marker for a post-equity-close minute carries the phase a captured row would have.
  4. `session.missed_slots`, D9's own day-by-day walk moved beside `skipped_slots` so both hooks share one enumerator and `lake.gap` needs no import of `lake.daemon`.
- **Unowned.** The backup-copy scrub, and the `--checksum` drop that waits on it. `manifest.scrub` reads under `lake_root` only, so nothing verifies the backup target today. `control_plane`'s Sunday gap list already names it as gap 3 of 5. The two land together or in that order, because until the scrub exists `--checksum` in `RsyncBackup.sync` is the only thing that would notice the backup rotting. Dropping it first trades a deadline that fails in a few years for a verification hole that starts now.
- **Unowned.** The daemon's in-loop close+15 compaction dispatch. `compact.compact` is reachable only from `python -m lake.compact`, and no job renders it. The design's rule that a catch-up compaction of an unsealed day is ordered after startup gap-marking is satisfied in-process today, because `run_loop` calls `on_start` before its first tick. Whoever builds the dispatch owns keeping it so. D11 built the seam it binds to, `session.SessionDispatch`, so what remains is the compaction job itself.
- **D11** close tags and the close+5 guard. Close+5 is the five-minute window after the option close, the last moment an option-close fetch may land. It plugs into D9's close-tag hook, and it builds the session-relative dispatcher the design calls for. Everything session-relative runs from inside the daemon, because launchd's calendar intervals are fixed wall-clock and cannot express a close-relative time. `SessionDispatch` fires one job once per session day at a moment the calendar decides, including on a daemon that starts after that moment has passed. The close+15 compaction dispatch binds to the same seam when someone builds it. Two rules are worth stating where both writers can see them:
  1. The guard's fill triggers on missing marks, not a missing cycle. A chain that failed at the option close leaves a tagged gap row holding nothing a reader can price against, and a close+5 refetch is exactly what rescues it.
  2. On a post-close restart the guard runs before startup gap-marking, so the two close minutes it owns are already recorded when D10's marker walks the day.
- **D12** compaction and backup, plus the nightly window re-tune. Compaction merges a day's segments into one sealed partition. The re-tune runs after it. The job groups the day's rows by `window_start` and `window_end`, compares each window's contract count to the body limit, and rewrites `chain_plan.json` when the profile drifts.
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

  The pasteable `INSTALL.txt` keeps the step-by-step procedure for operators who want it.
  It now leads with the composed command and carries the `sudo diff` line, and it points
  at `uninstall.sh`'s header for what an uninstall leaves rather than restating the list.
- **D15** query service with the Now and Today panels. The query service is the read-only localhost dashboard.

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
