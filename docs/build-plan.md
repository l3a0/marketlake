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
- **D10** startup gap-marking. Gap-marking writes an explicit marker for a missed minute, so a gap is recorded rather than silently absent. It plugs into D9's startup and skipped-slot hooks and reuses the last-durable-batch read the capture primitive's failure path already makes.
- **D11** close tags and the close+5 guard. Close+5 is the five-minute window after the option close, the last moment an option-close fetch may land. It plugs into D9's close-tag hook.
- **D12** compaction and backup, plus the nightly window re-tune. Compaction merges a day's segments into one sealed partition. The re-tune runs after it. The job groups the day's rows by `window_start` and `window_end`, compares each window's contract count to the body limit, and rewrites `chain_plan.json` when the profile drifts.
- **D13** watchdog and alerting. One test pins the startup rule. The per-surface counters start at zero, not rebuilt from the journal's last durable batch. So a restart never pages for the downtime that preceded it. Whole-daemon death stays the job of the external dead-man switch, the health ping whose silence pages. It plugs into D9's cycle-outcome observer.
- **D14** laptop control plane.
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

## When the health checks get created

Each healthchecks.io check is created by hand, in the session that first makes its job ping, and never earlier. A check begins its grace timer the moment it exists, so a row created ahead of its job pages for something unbuilt. The design doc's monitoring table holds the slug for each one.

1. **D8**, `slice1-capture`. The slice-1 runner's own envelope.
2. **D12**, `compaction`. The close+15 seal, backup, and re-tune.
3. **D13**, `capture`. The per-cycle dead-man. Delete the `slice1-capture` row in the same session, because `capture` supersedes it.
4. **D14**, `pre-open` and `sunday`. D14 renders the launchd jobs and the wake schedules those two checks watch.
5. **D16**, `eod-sweep`. The vendor sweep.

`calendar-probe` is the exception. No deliverable ships the 09:35 says-closed-but-open probe yet, so its row waits on whichever one does.

## Discipline rules

Two rules keep the pyramid upright. The pyramid is the shape of a healthy suite: many fast unit tests, fewer component tests, a thin layer of integration tests.

1. A bug's regression test goes to the lowest tier that can express it. A bug decided from values alone gets a unit test, never an integration test.
2. A new page-class path earns one unit test and at most one integration scenario. A page-class path is a code path that can raise a page-now alert. This caps the cost of each new alert.
