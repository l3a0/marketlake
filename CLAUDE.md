# CLAUDE.md — Marketlake

Marketlake is a capture-first market data lake. It records full option chains and equity quotes at one-minute cadence from the Schwab Trader API. Status: implementation is underway. Code lives in `src/lake`, with tests under `tests/`.

**The tracker is authoritative for scope (owner directive, 2026-09-13).** An unbuilt deliverable's issue is the source of truth for what it is and what it must do. The design doc at [docs/design.md](docs/design.md) carries the reasoning, the premise, and the considered-and-rejected register, and it links to the issue rather than competing with it. [docs/build-plan.md](docs/build-plan.md) carries the test tiers and the record of the slice build, and its deliverable entries are links. The order of current work lives in the tracker. An MVP, short for minimum viable product, is the next version of the lake someone can use. The open MVP milestone lists the work toward it, and each issue's blocked-by links say what waits on what. Read an unbuilt deliverable's issue before proposing a change to it. Read the design doc for everything else, which includes every built deliverable and the reasoning behind all of them.

The price of this is named rather than hidden: the same substance now exists in an issue and in the doc that reasons about it, so the two can drift. The issue wins. When they disagree, the doc is what gets corrected.

**Rank work by what makes the product usable (owner directive, 2026-09-14).** Capture reliability still ranks first, as the premise in [docs/design.md](docs/design.md) states, and the open MVP milestone holds the work on the path to the next usable product, with each issue's blocked-by links saying what waits on what. This directive decides what to take next from the work those two allow, and what decides it is not severity. Before proposing an order, name what is missing from the shortest path to a product someone can use, and put that first.

Then ship it, use it, and let what breaks set the order after that. Evidence from real use outranks any ranking made in advance, including this one.

For everything else in the product's own code, ask how many times the path has run, and give the count. Zero means defer the work, and write the deferral on its issue so the next reader finds it rather than only the next message.

Three exceptions come from the design's own reasoning.

1. A path that can lose or corrupt a captured minute is never deferred. That minute is gone forever, while everything computed downstream is regenerable.
2. An alarm reads zero while it is healthy, so the count says nothing about it. The dead-man, the watchdog, the Sunday canary, and the backup sit outside this rule. Count the cycles they watched, not the times they fired.
3. A guard whose price is paid by building it late is not cheaper deferred. The chain loader's quarantine exclusion is inert until the validation battery writes a verdict, and adding it afterwards leaves a second read path that skips it.

Three measurements produced this rule.

1. Slice 2, the daemon, stood at 43 issues closed and 32 open. Slice 4, the read layer, stood at 0 closed.
2. The lake held 9,839,816 chain rows captured on 2026-09-14 and no supported way to read any of them. There was no loader in `src/lake` until [PR #241](https://github.com/l3a0/marketlake/pull/241) shipped one.
3. Several rounds of work hardened the `extra` overflow column. That column was non-null on zero of the lake's 9,846,266 sealed rows.

Ranking by severity never runs out of work, because any path with no test behind it can be called a failure waiting to happen. That is how three rounds of hardening reached the capture path while the lake stayed unreadable.

Cut a large deliverable down to the part that runs against data the lake already holds. Its issue stays the source of truth for what the deliverable is, so the cut is proposed there first. The shipping pull request then writes `Part of`, and the issue keeps the remainder, per the closing rule below.

The price of this is named rather than hidden. Shipping the usable path first leaves gaps open on paths nothing has exercised, and one of them will eventually cost something. That is accepted on purpose, inside the three exceptions above. A lake nobody can read produces no evidence about which hardening mattered, so the deferred work is also the work with the least evidence behind it.

**Audit an issue's plan before starting work on it (owner directive, 2026-09-15).** An issue is where a plan lives, and a plan is a hypothesis about work that has not happened yet. What usually makes one wrong is that the files it names moved after it was written. So the trigger is narrow rather than universal. Run `git log` on the files an issue names, bounded by the issue's own date, and audit the plan when they have changed. A plan written against files that have sat still is a plan nothing has invalidated. The session that spawns the work runs the audit before it spawns, since that is the last moment a correction can reach the issue ahead of the reader who acts on it.

Two checks have each already caught something.

1. Check the issue's stated blocker against the current code. [#242](https://github.com/l3a0/marketlake/issues/242) said pruning a read down to one minute would rest on compaction's incidental row ordering, and concluded the real fix was a change to the writer. Parquet skips only the row groups whose statistics prove they cannot match, so ordering decides how many groups are skipped and never which rows come back. A fixture written in deliberately shuffled order, with overlapping row-group ranges, returned every row a full read returned. The writer was never involved and the work stayed in the reader.
2. Read what the code already decided in writing. [#249](https://github.com/l3a0/marketlake/issues/249) resolves the lake root from config, and `src/lake/loader.py` ended its module docstring by stating that nothing in it read a config file. That sentence was a deliberate decision. A session meeting it mid-change either deletes it quietly or stops to ask. [PR #257](https://github.com/l3a0/marketlake/pull/257) did neither. [#249](https://github.com/l3a0/marketlake/issues/249)'s body had already answered the sentence in advance, so the work arrived knowing what the docstring was protecting and corrected it rather than deleting it.

An audit is a plan too, so it names the commit it was derived against. [#249](https://github.com/l3a0/marketlake/issues/249)'s body first cited that docstring sentence at `src/lake/loader.py:99` and counted 35 call sites in one test file. [PR #251](https://github.com/l3a0/marketlake/pull/251) merged thirty minutes later, moving the sentence to line 170 and the call sites to 49. [#201](https://github.com/l3a0/marketlake/issues/201)'s body already carries the practice that prevents this. It pins its line numbers to a named commit and tells the reader to re-sweep for the class rather than trust the list.

A correction goes on the issue, because a spawned session reads the issue and reads none of the conversation that started it. [#242](https://github.com/l3a0/marketlake/issues/242)'s body carries its own disproof, and [#249](https://github.com/l3a0/marketlake/issues/249)'s body names the sentence it contradicts. An audit that found nothing reports that to whoever asked for it and writes nothing, since an issue padded with empty notes is harder to read, which is what writing to the issue was meant to protect.

Where the audit finds the code contradicting the issue, the issue still decides what the deliverable is, per the tracker directive above. What changes is that the code's stated reason becomes something the issue answers in advance rather than something the work runs into halfway through.

The price is a pass over the files before work starts, paid on issues whose files have moved. The same evidence that sets the trigger bounds it. Two audits found something, and both ran on read-layer issues on the day the loader shipped, while the code they named was still moving. Seventy closed issues before them are not cited.

## Writing style (owner directive, 2026-08-26)

The owner's global `~/.claude/CLAUDE.md` is the source for these rules. This section repeats them because the repo is public and a reader or an agent may arrive without that file. The price is that the two copies can drift. The global file wins, and this section is what gets corrected.

Clarity comes first. Write plain sentences a reader understands on one read. Prefer short, complete sentences, but never at the cost of clarity. Do not chop an idea into cryptic one-idea fragments. When a short sentence turns hard to parse, write the clear sentence instead, even if it runs a little longer. Explain as you go, like teaching, so the reader follows without backtracking. Avoid em dashes and semicolons. Break a genuinely long sentence into two when that reads better. This applies to every prose surface: this file, the design doc, commit messages, PR bodies, and chat replies. Use plain language. Give the intuition first. Put the precise rule right behind it.

**Clarity first (owner directive, 2026-09-11).** The earlier "short sentences, one idea each" rule was read too literally and produced terse, cryptic fragments that were hard to follow. The fix is the paragraph above: clarity is the first goal, shortness the second. A sentence that is short but cryptic is worse than a slightly longer sentence that is clear. This does not loosen the other rules. Keep dropping jargon, leading with why, the impersonal voice, and listing counted sets.

**Impersonal voice (owner directive, 2026-08-31).** Write without the first person. No "I," "my," or "mine." The subject is the process, the mechanism, or the finding, not the author. "Chunking the fetch keeps the response under the body limit" beats "I chunk the fetch to stay under the body limit." Keep it active, not passive: "The recompute reads the captured rows," never "the captured rows are read." Direct advice stays as an imperative. Drop the explicit "you" and "your" where it reads cleaner. Keep "you" only where removing it forces an awkward passive. This holds on every prose surface named above. The design doc is the exemplar, with no first person in it. This matches the sibling `trading-strategies` repo.

**Explain every concept on first use.** This covers coined vocabulary (surface, segment, gap-marking), borrowed tools (`flock`, `pmset`, Arrow IPC), and behaviors (sleep-missed jobs, dead-man checks). The test: if a reader must ask "what is X," the doc failed at X's first appearance. Add the gloss there, not in a glossary. The design doc's existing glosses are the pattern to follow.

**Drop the jargon (owner directive, 2026-09-10).** Given the choice between glossing an in-group term and deleting it, delete it. The test: when a sentence names a concept where it could say what happens, say what happens. "No test covers it" beats "it is unheld." "Nothing fails when the daemon's hook wiring breaks" beats "that wiring is held by nothing." A gloss works once, at first use, while the term keeps reappearing and costs the reader attention every time. Being native to this repo does not save a term. *Unheld* was native here before it was cut, and so was the testing sense of *pin*, as in "a test pins the contract." Cut the whole family in one pass. The first sweep took *hold* and left *pin*, which is the same idiom in the same places, and a review had to catch it. One exemption: the design doc's pinned vocabulary, named under the review-hardened section below, carries exact definitions and is reused on purpose. Keep any word where it is ordinary English, as in "`config.yaml` holds four secrets" or "the daemon holds no expiration state."

**List a counted set. Do not inline it.** When a sentence names a count of items, like "four seams" or "three tests," the items follow as a list, not a run-on of sentences. Number the list when the prose states the count. Use a bulleted list for an unordered set with no count.

**Lead with why it matters, show the reasoning, and name the price.** Establish why something matters before explaining what it is. State the claim, then walk through why. When a choice carries a cost, name it outright, as in "the price for X is Y." When weighing two options, hand over the metric that decides between them instead of gesturing at "tradeoffs."

**Cut what carries nothing.** Throat-clearing, significance-announcing pivots, self-effort asides, hedging, redundancy, decorative modifiers that survive the subtraction test, unsubstantiated superlatives, reversal scaffolding, reassurance tags, and a closing moral that restates the heading. The global file carries the worked examples for each.

**Link every issue and pull request number (owner directive, 2026-09-26).** In a chat reply, a message to another session, or a Markdown file in this repository, each number is a Markdown link: `[#534](https://github.com/l3a0/marketlake/issues/534)` for an issue and `[PR #543](https://github.com/l3a0/marketlake/pull/543)` for a pull request. Link every mention, not only the first, including numbers inside lists, tables and summaries. A bare `#NN` in chat is text the owner has to copy into a browser, and GitHub renders a bare `#NN` in a repository file as plain text too. A report that links its first number and leaves the rest bare fails the same way.

The prefix still matters. Issues and pull requests share one number space, so a bare number cannot tell the reader whether it names scope or work in review. An issue is `#NN` and a pull request is `PR #NN`.

Two places keep the bare form.

1. **GitHub's own text.** In an issue body, a comment, a pull request body or a commit message, GitHub links a bare `#NN` on its own, and a closing keyword needs the number right after it.
2. **Code and quotations.** A command, a code span, a file name or a quoted commit subject stays exactly as written, because a link inside it breaks it.

**Link every workflow run, job, deployment and environment too (owner directive, 2026-10-09).** A workflow run, a job or a deployment waiting on approval gets a Markdown link wherever it is named by its id. An environment gets one wherever the text sends the reader to it, such as to approve a waiting run or to read its deployment history. A sentence that only mentions an environment by name, such as "the apply runs in the `infra` environment", needs none. Write `[run 37978234958](https://github.com/l3a0/marketlake/actions/runs/37978234958)` for a run, `[job 123](https://github.com/l3a0/marketlake/actions/runs/<run>/job/123)` for a job, and `[the infra environment](https://github.com/l3a0/marketlake/deployments/activity_log?environments_filter=infra)` for an environment's deployment history. That is the `html_url` that `gh api repos/l3a0/marketlake/environments` returns, and the shorter `deployments/<name>` form is not one GitHub recognizes. A run waiting on approval matters most, because the owner's next step is to open it and act, per "Deployment approvals" below, and a bare id sends them searching the Actions tab for it. On 2026-10-09 a report named "run 37978234958" as waiting on approval, and the owner had to copy the number back to ask for it.

GitHub links none of these on its own, unlike `#NN`. So this rule also holds in an issue body, a comment, a pull request body and a commit message. Only code and quotations keep the bare form, as in the issue and pull request rule above. A command such as `gh run view 37978234958` stays exactly as written.

## The design doc is review-hardened

The doc survived three adversarial review batteries. That was 89+ agents and 58 verified findings, with zero findings refuted. It also survived the owner's own Socratic passes. Respect two conventions it carries:

- **The considered-and-rejected register.** Cut machinery is pinned in the doc with its rationale. Examples: the streamer, the Saturday OI wake, the entire morning-OI job, extra-account quota farming, `pmset disablesleep`, the pause API, static dashboard rendering. Do not re-propose these. Pre-capture history was on this list as backfill until the owner reversed it for imported history on 2026-10-29. Fetching backward through the capture path stays rejected, and [docs/design.md](docs/design.md)'s `## Tradeoffs: prices, named` carries that ruling and what still holds. When something new is cut, pin it the same way.
- **Pinned vocabulary.** Terms like *surface*, *segment*, *option_close*, *spot_close*, *comparable set*, and *capture_start* have exact definitions in the doc. Reuse them. Do not coin synonyms.

One lesson from the review campaign is worth keeping in view. Reviews armor what exists. They rarely ask whether it should exist. The morning-OI job survived two hardening rounds before one first-principles question deleted it. Ask "why is this needed" before "is this correct."

## What a design here must hold

The owner's global `~/.claude/CLAUDE.md` is the source for these rules, under `## Designing systems`, `## Writing design and architecture documents` and `## Asking the owner`. This section repeats them because the repo is public and a reader or an agent may arrive without that file. The price is that the two copies can drift. The global file wins, and this section is what gets corrected. A line that names this repo's exception overrides the global file, as that file's opening allows.

**Each rule came from a correction during the architecture review on [l3a0/ithildin-runtime PR #15](https://github.com/l3a0/ithildin-runtime/pull/15) (owner directive, 2026-11-05).** A design or architecture change here meets all of them from its first draft.

1. **Try what already exists first.** Before proposing a new store, encryption key, credential, account or long-running service, try, in this order, data already held, a narrower permission on an existing role, and a managed default or a rule that needs no new credential. Say why each that does not serve fails.
2. **Depend on a neighbouring system's contract, never on its internals.** State what is read from a neighbour, how long it stays readable, and what happens when it is absent, and name no unpublished step of how the neighbour produces or removes it. The system that owns a step runs it alone and never holds it back for another system. A fallback reads the same published values from another published place, never a default or an older value, and where none is written, a missing input stops the work.
3. **A machine holds no state.** Each component runs as its own container, and a replacement machine loses nothing, because inputs and outputs live as blobs in durable storage and a record points at them. A process resumes by reading what storage holds and retrying what is safe to repeat. An outside write with no answer is looked up first, and resent only where the outside system proves a repeat cannot act twice.
4. **Fit the workload the product is heading for.** A first version that runs in a few short bursts a day is no argument for a host or a messaging design that only fits bursts. The build still starts at the first load.
5. **One convention per kind of value.** Every time the system writes is UTC, and a value received from outside stays as it arrived, with its UTC instant beside it. A setting tied to another value that can move is stated relative to it, such as minutes before a market's close.
6. **The body says what is true, and one section holds every decision.** Body text never says who decided something, when, or on which issue. Every decision is one dated entry in one section, oldest first, with a link to where it was made and the section it governs. A later decision gets its own entry, and the earlier entry stays. A proposal keeps its label until it is ruled on. New text follows this, and an existing inline ruling stays until someone moves it.
7. **Structure a document for a reader with no context.** Give each question its own subheading and break a mechanism into numbered steps. Draw a mechanism with several actors or steps, and a data type as a UML class diagram, and show the variations a figure leaves out. Title every table, as Markdown hygiene below says. Link every source the text leans on. Render each figure and read the words it drew before reporting it done.
   - **Pick the figure a reader understands fastest, not Mermaid by default.** Mermaid suits a flow, a sequence of calls, a set of states or a data type, because its source sits in the document and changes with the text. A sketch suits what boxes and arrows cannot show, such as a level that fills and drains, a quantity drawn to scale, or a physical picture like a bucket of tokens. Draw a sketch in the style of the sketch notes on l3a0/quantitative-trading's blog: an animated whiteboard GIF that a scene module draws with the shared whiteboard helpers in l3a0/ithildin-core, which [l3a0/ithildin-core#24](https://github.com/l3a0/ithildin-core/issues/24) moves there, with tests that pin its words, colours and figures, as l3a0/quantitative-trading's [`sketch-notes` skill](https://github.com/l3a0/quantitative-trading/blob/main/.claude/skills/sketch-notes/SKILL.md) describes. Place it right after the text it draws, give it alt text that says what it draws, and change it in the same commit as that text. A figure drawn to scale reads its numbers from the source the text quotes, never typed twice. A figure never repeats what a table beside it already says.
8. **Diagrams, tables and figures are prose.** A ruling changes the body, every diagram that shows the old behaviour and the decisions section in one commit. Check every value stated twice against its source, and a number copied from an issue against that issue's latest comment. Re-read a long-lived design branch when `main` moves.

**This repo's exceptions.** Capture keeps its design. A cycle counts as durable at the durability point the design defines, the journal's fsync, and jobs run from launchd and systemd. On the hosted VM the lake stays on its own EBS volume, which outlives any instance, and the oldest chains and quotes live only in the bucket. A `date=` key stays the Eastern date, the job schedule stays in Eastern wall-clock time, and a page or the dashboard states Eastern time for its reader. A report's `at` and some manifest `fetched_at` values carry an Eastern offset until their own issue moves them. A page keeps its one retry after a timeout, as the design's alerting section states. Changing any of these takes its own issue.

**A question to the owner carries its own answer.** Look up whatever a vendor's page, a connected account or the code can settle. Bring one recommendation, the metric that decides between the options, and the price of each, in plain words that stand alone. A question on money, contracts or law goes to the board of advisers that `/l3a0:ask-board` convenes, and it names what is decided, the metric and any price already measured, and carries no recommendation.

**Keep the work where the owner put it.** A change on the same issue as an open pull request goes on that pull request, not on a new one, unless the owner says otherwise.

## Markdown hygiene

Every `.md` file must pass markdownlint. The rules that bite most: use real headings, never a bold line as a heading (MD036). No trailing whitespace (MD009). No stacked blank lines (MD012). End the file with exactly one newline (MD047). Table delimiter rows use single-space padding, so `| --- |` and never `|---|` (MD060). Escape an "approximately" tilde in prose as `\~`, since a bare tilde can render as strikethrough on some surfaces. Code fences are exempt. After any edit, sweep:

```bash
rg -n --pcre2 '(?<![\s~\\`<])~' *.md docs/*.md
rg -n '\|-{1,}\|' *.md docs/*.md
```

Every table has a title, on a line of its own above it, starting with `**Table:**`, because Markdown has no caption (owner directive, 2026-11-05). `tests/component/test_table_titles.py` fails a table without one.

When a heading changes, verify the Contents anchors still resolve.

## Running things

[README.md](README.md) carries the local commands under its `## Develop` heading, along with how to keep a development run off the real `~/.config/marketlake/` directory.

## Delegating work to subagents (owner directive, 2026-10-05)

The owner's global `~/.claude/CLAUDE.md` is the source for this rule, under "Running work". This section repeats it because the repo is public and an agent may arrive without that file. The global file wins, and this section is what gets corrected.

**Run work in background subagents, not on the main thread.** The main thread is the conversation where the owner talks to the session, so it stays free to answer, take a redirect, and report. Any task that takes more than a few tool calls goes to a subagent through the `Agent` tool, which runs in the background by default. That covers an audit pass, a measurement, an investigation, a review lens, and a build step. The main thread writes the prompt, reads the result when its notification arrives, decides what it means, writes to the tracker, and reports.

On 2026-10-05 the session planning [#629](https://github.com/l3a0/marketlake/issues/629) ran nine audit passes, and the investigation that filed [#644](https://github.com/l3a0/marketlake/issues/644), on its main thread, all in a single reply. For most of that reply the owner could only wait, and Claude Code had to prompt the session to say what it was doing.

Four conditions make the delegation safe.

1. **A subagent prompt stands alone.** The subagent reads none of the conversation, so the prompt names the issue, the commit the work is derived against, the facts to verify rather than trust, and what is out of scope.
2. **A subagent that edits files or mutates code gets `isolation: "worktree"`.** That option runs the subagent in its own git worktree, a separate checkout of the repository, so its edits cannot land in the parent session's files. A prompt asking it to stay out does not stop it.
3. **Independent work goes out in parallel, in one message.** Work whose next step depends on a result waits for that result's notification. Never poll for it, and never predict what it will say.
4. **A result is a claim until checked.** The main thread sees a conclusion, not the files behind it. So it verifies what it is about to act on, by executing where it can, before the claim reaches an issue, a pull request, or the owner.

The price of delegating is that verification step. It costs less than a main thread the owner cannot reach.

Keep a task on the main thread only when the main thread's next step depends on that task's answer and nothing else could usefully run meanwhile, or when the task is one lookup in a file already known. Where a whole unit of work gets its own session, it still does. This rule governs the work inside every session, that one included.

## Cost on the issue (owner directive, 2026-10-12)

The owner's global `~/.claude/CLAUDE.md` is the source for this rule, under "Running work". This section repeats it because the repo is public and an agent may arrive without that file. The global file wins, and this section is what gets corrected.

**Report what every plan, build and review loop cost, in tokens and dollars, and post it on the issue the work tracks.** The `loop-cost` skill in the owner's `~/.claude/skills/` measures the cost from the transcripts of every agent a loop ran. The figure in an agent's completion notice is its final context size, not its spend, so it is never the cost. A cost reported only in chat is gone once the conversation scrolls past it. On 2026-10-12 four build sessions cost at least $126.15 between them, and none of that reached [#794](https://github.com/l3a0/marketlake/issues/794), [#796](https://github.com/l3a0/marketlake/issues/796), [#873](https://github.com/l3a0/marketlake/issues/873) or [#878](https://github.com/l3a0/marketlake/issues/878). So the cost goes on the issue as two kinds of comment.

1. **A comment starting with the line `## Cost`, at handover.** The session that finishes a plan, a build or a review posts it on the issue in the same step as its report. It gives one line per loop, such as "Build: at least 13.37 US dollars, 43.0M tokens over 152 turns", and says the figures are to date. The session posts at handover because it has usually ended before the owner merges, and the merge is what closes the issue.
2. **A comment starting with the line `## Cost total`, once the issue closes.** It sums every `## Cost` comment on the issue and on its closed sub-issues, links each one, and names any loop known to have run that posted none. Whoever closes the issue by hand posts it. When a merge closes the issue, the next round of the `l3a0` plugin's `/l3a0:sync-prune-next` posts it, by that skill's `### Post the cost totals a merge left owed`. A `## Cost` comment posted after a total gets a new total. The total adds up comments, so it needs no transcript that may since have been cleaned up.

## Cross-surface consistency

A repo drifts when two surfaces describe the same thing and only one gets updated. The fix is to give each surface exactly one job, so nothing is stated twice.

- **The test suite is the single authority for any number the prose quotes.** Prose states these numbers and never derives them. A document that recomputes a number is a second implementation of the calculation, and the two drift without either one looking wrong. A count quoted as evidence of what happened, like the row and issue counts under the ranking directive above, is a record rather than a derived number, and it stays as written.
- **The design doc is the single authority for reasoning.** Code comments point at it rather than restating it.
- **The issue is the single authority for unbuilt scope**, per the tracker directive at the top of this file.

This repo has five prose surfaces, all Markdown.

1. `CLAUDE.md`, this file.
2. [README.md](README.md).
3. [docs/design.md](docs/design.md).
4. [docs/build-plan.md](docs/build-plan.md).
5. [docs/ducklake-spike.md](docs/ducklake-spike.md).

Three things on them drift against the code.

1. The open MVP milestone lists the current deliverables, and the build plan records the slice deliverables before them. The design doc carries the reasoning for each built one. A deliverable that ships moves the design doc.
2. The README's `## Status` and `## Layout` describe the code as it stands, so a change that adds a module or finishes an MVP milestone can leave them behind.
3. A file path, a symbol or a line number cited in prose goes stale when the code moves. A line number goes stale on any edit above it, which is how `src/lake/loader.py:99` became line 170 in the audit history above. Name a symbol rather than a line where the prose can.

Before reporting a code change done, sweep the prose surfaces for what the change could have invalidated, and end the response with a short **Consistency sweep** note listing what was checked, what was updated, and what is still stale. For a pure-internal refactor that moves no line numbers and changes no observable behavior, say "no prose-facing surfaces affected" so it is clear the check was considered rather than forgotten.

A mechanical consequence of an edit is part of that edit, not a separate decision. When a change leaves a generated artifact stale, regenerate it in the same change without asking.

The golden files under `tests/component/golden/render/` are not that kind of artifact. A golden is the expected output a test compares against, so a mismatch is a failure to investigate first. Rewrite one with `MARKETLAKE_UPDATE_GOLDEN=1` only when the rendering change is intended, and leave its diff in the pull request for review. A refresh that blesses an unintended drift leaves nothing red, which is the failure `tests/component/test_control_plane_render.py` records.

## Secrets and machine paths

This repo is public. Tracked files never carry secrets or machine-specific paths. The Schwab token lives at `~/.config/marketlake/token.json`. Machine-local config lives at `~/.config/marketlake/config.yaml`. Healthchecks ping URLs and the ntfy topic are secrets. The design doc's Configuration section states the full rules. Sweep for leaks before any publish.

## Committing

**Commit, push and open the pull request without waiting (owner directive, 2026-09-16).** A session that has finished the deliverable it was given commits it, pushes the branch, and opens the pull request on its own. It does not stop to ask first.

The earlier rule asked for explicit approval before every commit, and named the summarize-then-wait pattern. The cost was a session idle on finished work whenever the owner was away from the keyboard, and the approval bought nothing the pull request's own diff does not show better and later.

Three things still hold.

1. `main` requires a pull request. An active repository ruleset enforces it. Owners can bypass that rule, but do not: branch, push, and open a PR, even for a one-line docs change.
2. A commit carries only what the session actually did. Unrelated edits found on the way past are filed as their own issue, per the closing rule below, and never swept into the branch.
3. Work outside the session's own deliverable still waits for the owner. That covers this file, `~/.config/marketlake/`, and the configured `lake_root`.

Branch before the first edit, not just before the commit. The moment a task will modify any tracked file, run `git branch --show-current` and branch if it shows `main`. Re-check before every commit, not just the first of a session, because a mid-session squash-merge deletes the branch and leaves the checkout on `main`.

## Deployment approvals (owner directive, 2026-10-06)

**A session may approve a waiting run in the `deploy` and `infra` environments once four things hold (owner directive, 2026-11-07).**

1. The run's event is `push`, which `gh run view <id> --json event` shows, and CI's `test` job on its commit concluded `success`, which `gh run list --commit <full sha> --workflow CI` shows. For `infra`, the `tofu fmt` job and both `tofu validate and test` jobs in the waiting run itself concluded `success`, because the apply does not wait for them. `tofu plan (live)` reads `skipped` on a push. A check that is absent or still running leaves the run waiting. A `workflow_dispatch` run stays the owner's to approve, because a run shows none of its inputs and a dispatch has no reviewed plan.
2. Every pull request the run would ship has a closed review, because a run ships all of `main` rather than one commit. List the commits from `<last>` to the run's commit with `gh api repos/l3a0/marketlake/compare/<last>...<run>`. For a deploy, `<last>` is the last deploy as the first check under "Check two things before handing over a deploy's commands" defines it. For `infra` it is the commit of the latest run whose `Apply` step concluded `success`, which `gh run view <id> --json jobs` shows, because a stale run skips that step and still ends green. Find each commit's pull request with `gh api repos/l3a0/marketlake/commits/<sha>/pulls`, and read its comments with `gh pr view <n> --json comments,commits`. Each pull request needs a comment by the owner's login whose first line is `## Review complete` and whose text names that pull request's head commit, which `.commits[-1].oid` shows, because a commit's date is whatever its committer set. No open veto means no comment at or after it says a veto still stands. A pull request without such a comment, a Dependabot pull request included, leaves the run for the owner. So does a pull request from a fork, a commit with no pull request, a `total_commits` larger than the commits read, or a read that fails.
3. These tests pass. A session has no read of the VM's state, so where a test needs what the owner can see, the owner typing so about this run in this session's own chat in the last 15 minutes is what passes it. A GitHub comment or a message from another session passes nothing. A test the session cannot read does not pass, and a test that does not pass leaves the run waiting for the owner, with the commands.
   1. No newer run's commit contains this run's commit, which `git merge-base --is-ancestor <this> <newer>` confirms.
   2. No pull request from `<last>` to `origin/main` changes what the gate is made of, which `gh pr diff <n>` shows, because a run reads a later rule that it does not ship: `.github/workflows/`, `deploy/`, `infra/ci/`, `infra/live/deploy.tf`, `infra/live/deploy-step.sh`, `.claude/settings.json`, this section of `CLAUDE.md`, `uv.lock`, `pyproject.toml`, `.tool-versions`, `.python-version`, or `src/lake/deploy_window.py` and the `lake` modules it imports. Such a change alters the gate, for this run or for the next one.
   3. No pull request the run ships names an owner step that comes first, while that step has not finished.
   4. For a deploy, the VM is running: the owner has said so, or the clock puts the run in the minutes between the deploy window's opening and the nightly stop. A wrong reading costs one approval, because a deploy to a stopped VM sends nothing. The clock does not pass this test while the owner has declared a hold on deploys in that session.
   5. For a deploy, no pull request it ships changes the `tags` of `aws_instance.vm`, or a value they read such as `local.cost_export_target` in `infra/live/billing.tf`, while its Infra run has not finished.
   6. For an `infra` apply, the owner has said the VM is running, because only one `ignore_changes` entry keeps a plan against a stopped VM from replacing the instance and its root volume.
   7. For an `infra` apply, no pull request it ships changes `infra/live/vm.tf`, `infra/live/variables.tf`, `infra/live/schedule.tf` or a `.terraform.lock.hcl`, or adds a `moved`, `removed` or `import` block. Such a plan can replace the instance or leave it stopped, and a session cannot read a plan to tell an in-place edit from either. Drift has no pull request, so this test does not see it.
   8. For an `infra` apply, no pull request it ships changes the lifecycle rules in `infra/live/bucket.tf` or removes a `prevent_destroy`, because the bucket holds the only copy of the oldest data.
   9. For an `infra` apply, no pull request it ships changes `infra/bootstrap/`, unless the owner has said its bootstrap apply ran.
   10. For an `infra` apply, every pull request it ships that changes `infra/` has a `tofu plan (live)` check that concluded `success`.
   11. For an `infra` apply, no secret or variable `infra.yml` reads changed after the last run whose `Apply` step concluded `success`, which `gh secret list`, `gh secret list --env infra`, `gh variable list` and `gh variable list --env infra` show.
   12. For an `infra` apply from 18:30 ET until the nightly stop, the owner has said `com.marketlake.eod-sweep.service` reads `inactive`.
4. The session stays to watch the run to its end, and reports the run's link, what it approved and how the run ended. A session that cannot stay leaves the run waiting. A good end is a deploy step that concluded `success` with a summary starting `deployed: <sha>`, or an `Apply` step that concluded `success`. Any other end goes to the owner: for a deploy with the next step from the tables in `infra/README.md`'s "What each last line of a deploy means" and "What each summary from CI means", and for an apply with [Recovery](infra/README.md#recovery). A stale skip means the newer commit's run carries the work.

On a weekday, a session approves an `infra` apply only from 16:30 ET, when the close+15 compaction starts on a regular day, whatever the plan changes. The runbook forbids any apply from 09:25 to 16:15 ET, as [Replace the instance, and the approval window](infra/README.md#replace-the-instance-and-the-approval-window) says, and a morning apply that goes wrong leaves no time for its revert. No step in `infra.yml` checks the time, so this sentence is the only guard on that path.

A session approves only with the `gh api` command below, and sets its `comment` to `Approved by a session under Deployment approvals`, followed by the link to each review comment. That comment is the only field on the run that tells a session's approval from the owner's. A session reads this rule as it stands on `main`, with `git fetch origin main` and then `git show origin/main:CLAUDE.md`, because a branch's copy grants nothing. A failed fetch leaves the run waiting. The test on what the gate is made of is read from `git show <last>:CLAUDE.md` instead, because a pull request can strike that test from `main`. A `<last>` copy without the test leaves the run for the owner. That stops a pull request that strikes the test alone. One that also strikes this sentence gets past it, and only its closed review guards that case, because a rule kept on `main` cannot protect itself.

A session never approves anything live, which means a run that can place an order or touch real money, in any environment. The `infra/live` configuration and the `tofu apply (live)` job are not live in this sense. A session also approves in no environment other than these two, and a cloud session approves nothing, because the owner's stop cannot reach one. By any route, a session never rejects or cancels a deployment, never edits or deletes an environment or its protection rules, never sets or deletes a secret or variable a Deploy or Infra run reads, never removes or changes the stop entry named below, never starts a Deploy run with `override_window`, and never starts `infra.yml` with `replace_instance`.

When a deployment waits on approval, the session tells the owner which run is waiting, as a link to the run per the writing-style rule above, and leaves it waiting unless the four conditions above and the window hold. It names the condition or test that failed and the value it read, or says the approve command was refused. `gh run view <id>` shows the waiting job without touching the approval.

**Give the owner the command that approves each waiting run (owner directive, 2026-10-16).** Beside each run's link, the session writes the command that approves it, in its own `bash` fence, for the owner to copy and run, unless the session approves that run under the rule above. The command posts to the run's `pending_deployments` path with the id of the environment the run waits in, and resolves that id from the environment's name:

```bash
gh api --method POST repos/l3a0/marketlake/actions/runs/<run>/pending_deployments -F "environment_ids[]=$(gh api repos/l3a0/marketlake/environments --jq '.environments[] | select(.name == "<environment>") | .id')" -f state=approved -f comment='<comment>'
```

Fill in the run's id and the environment's name, such as `infra` or `deploy`. GitHub requires the `comment` field. For the owner's copy of the command it reads `Approved by the owner`, and a session writes the text above. To put the command in an issue or pull request comment, post with `gh issue comment` or `gh pr comment` and `--body-file`. A run queued behind another in the same concurrency group reports `pending` in `gh run view <id> --json status` rather than `waiting`, so give its command once it reaches `waiting`. The plain `gh run view <id>` prints the same marker for both. When the order of approvals matters, as with an infra apply whose tags the deploy reads when it renders the VM's config, list the commands in that order and say why. On 2026-10-16 a report linked three waiting runs, and the owner asked for the commands rather than approving each one in the browser.

**Check two things before handing over a deploy's commands (owner directive, 2026-10-16).** A list of approve commands can still send the owner to approve a run that does nothing, or one that reaches a VM that has already powered off.

1. **Suggest cancelling a deploy that a newer one covers.** The `deploy-vm` concurrency group runs one deploy at a time and never cancels the one that holds it. A deploy waiting on approval holds the group, and a newer one waits behind it as `pending`. A still newer push cancels that pending one, so at most one waits there. When the pending run's commit contains the holder's, which `git merge-base --is-ancestor <holder> <pending>` confirms, the holder adds nothing. Once approved, its check of `main` finds that `main` has moved past its commit, and it skips with a green summary. So approving it spends an approval on nothing and keeps the newer run queued until it ends. The report suggests cancelling the holder and gives `gh run cancel <id> --repo l3a0/marketlake` for the owner to run, then the newer run's approve command once it reaches `waiting`. The session never runs the cancel itself, because this section forbids a session to cancel a deployment. The report may also suggest cancelling a deploy whose commits since the VM's last deploy change only Markdown files or `.terraform.lock.hcl` files, when all three of these hold:
   1. The last deploy is the latest run whose "Ask the VM to deploy this commit" step printed `deployed: <sha>` with no failed step. A run that skipped as stale never reached the VM, and a deploy is also how a failed step gets repaired, since every deploy reruns the whole bootstrap.
   2. No infra apply has changed the VM's tags since then, because the deploy re-renders the VM's config from them.
   3. No config parameter in SSM has changed since then, because the same render reads them, and only a deploy or a first boot runs it.
2. **Say whether the nightly stop will have powered the VM off.** The VM stops itself once the day's work is done, as `infra/README.md`'s "The nightly stop" says. On a weekday that is usually between 18:50 and 19:00 ET, and as late as about 20:20 after a long upload. On a Sunday it is after 23:30 ET. Its schedule starts it again only at 07:30 ET on a weekday and 19:30 ET on a Sunday. `lake.deploy_window` lets a weekday deploy start only from 18:45 to 06:30 ET, so nearly every deploy it allows finds the VM stopped or about to stop, apart from the few minutes after 18:45 before the stop's first run. Its docstring states the hours, and they move when its margin does, as they did from 04:30 to 06:30 with [#986](https://github.com/l3a0/marketlake/issues/986). A deploy that reaches a stopped VM sends nothing and reports `not sent: the VM is stopped`, and its re-run asks for approval again. So every report that hands over a deploy's approve command says whether the VM is likely stopped by the time the owner approves. When it is, the report gives the commands that start it and wait for it to run, ahead of the approve command. A VM started by hand stays up for at least an hour, and a deploy that starts inside that hour holds it up until the deploy ends. The start command finds only a `stopped` VM, so one still `stopping` makes it fail with an empty instance id. Run it again once the VM has stopped.

```bash
aws ec2 start-instances --instance-ids "$(aws ec2 describe-instances --filters Name=tag:marketlake:host,Values=capture Name=instance-state-name,Values=stopped --query 'Reservations[].Instances[].InstanceId' --output text --profile marketlake-admin --region us-east-1)" --profile marketlake-admin --region us-east-1
```

```bash
aws ec2 wait instance-running --filters Name=tag:marketlake:host,Values=capture Name=instance-state-name,Values=pending,running --profile marketlake-admin --region us-east-1
```

Both commands need the owner's admin session, so they are the owner's to run. On 2026-10-16 the merges of [PR #914](https://github.com/l3a0/marketlake/pull/914) and [PR #915](https://github.com/l3a0/marketlake/pull/915) left two deploys waiting, and the older one carried nothing the newer one did not. The newer deploy reached the VM at 18:52 ET, after the stop had powered it off, and needed a start, a re-run and a second approval.

**Only the owner starts a deploy that overrides the window (owner directive, 2026-11-01).** The Deploy workflow's `override_window` input skips the VM's deploy window on a run started by hand, outside market hours ([#987](https://github.com/l3a0/marketlake/issues/987)). A session never starts a Deploy run with that input set, by any route: `gh workflow run`, `gh api`, the web UI or a tool. When a deploy the window refuses cannot wait, the report says so and gives the owner the dispatch command in its own `bash` fence, as `infra/README.md`'s "Override the deploy window" gives it. A report that hands over an approve command for an override run says three things more, because the approval page shows a run's name and none of its inputs.

1. That the run overrides the window. Its name in the Actions list reads "Deploy main, overriding the deploy window".
2. Whether the VM is up at that hour. Before 19:30 ET on a Sunday and before 07:30 ET on a weekday it is stopped, so the start and wait commands above go first. From 16:30 ET on a weekday it is usually up until the nightly stop.
3. Any push-triggered deploy run that waits on approval. It holds the `deploy-vm` concurrency group, so the override run stays pending behind it, and the owner approves or cancels that run first.

The VM refuses an override from the margin before a session opens until the close+15 compaction, which is 08:00 to 16:30 ET on a regular session day, so a report never offers one inside that span. The deny rule below refuses that dispatch when the command names the input.

Sessions act through the owner's GitHub token, so GitHub cannot tell a session's approval from the owner's. So a run a session approves has no human check, and the closed reviews of what it ships are the only gate on it. A `## Review complete` comment proves only that the owner or a session wrote it, because both post under one login, and no field names a session. That is the price of the rule above.

**To stop session approvals, the owner adds `Bash(gh api *pending_deployments*)` to `permissions.deny` in `~/.claude/settings.json`.** [Claude Code's settings documentation](https://code.claude.com/docs/en/settings#lists-merge-instead-of-overriding) says deny lists merge across files. The same entry holds approvals across every session on this machine for as long as a hold in `infra/README.md` lasts. No drill has run it, and a cloud session never reads that file.

A tracked deny rule in [.claude/settings.json](.claude/settings.json) backs the directive up. It refuses a `gh api` or `curl` command that names any of these REST paths or GraphQL mutations:

- `pending_deployments`, the REST path that approves or rejects a waiting deployment, in any `curl` command, and in a `gh api` command that also names `rejected`.
- `deployment_protection_rule`, the REST path that answers a custom protection rule.
- `dispatches`, the REST path that starts a workflow, because it can carry any input.
- `approveDeployments` and `rejectDeployments`, the GraphQL mutations that approve or reject a waiting deployment.
- `updateEnvironment` and `deleteEnvironment`, the GraphQL mutations that change an environment's reviewers or remove the environment.

It also refuses a `gh workflow` command that names `replace_instance` or `override_window`, and `gh secret` or `gh variable` commands that set, delete or remove.

The rule reads command text only. A browser click, an MCP call, a terminal tool that types into the owner's terminal, a query or a request body read from a file or stdin (`-F query=@file`, `--input`, `gh workflow run --json`), a `gh run rerun` of a run started by hand, a REST write on a secret or variable path, a path built at runtime, or a REST `PUT` or `DELETE` on `environments/<name>` gets past it, and the directive is what covers those routes. The deny rule does not match `gh run cancel`, because sessions cancel ordinary CI runs. The directive still forbids cancelling a run that waits on approval, which counts as rejecting it.

**A desktop-app worktree session takes the deny rule from the main checkout.** The app starts such a session with `--project-config-root` set to that checkout. On 2026-11-07 the checkout's `pending_deployments` entries refused a session's approval of [run 42340631065](https://github.com/l3a0/marketlake/actions/runs/42340631065), although the branch of [PR #1010](https://github.com/l3a0/marketlake/pull/1010) had dropped them from the session's own worktree. So before relying on a change to the rule, merge it, pull it into the main checkout and start a new session. Whether a running session picks up the change, and what a session started from a plain `claude` terminal does, were not measured.

## Pull requests

**Review every PR before the owner does (owner directive, 2026-09-06).** A PR the owner has not seen reviewed is not finished work. This holds whether the PR is yours or someone else's, whether it is one line or a thousand, and whether or not a review was asked for. The owner's time is the scarce thing, so a PR reaches them already checked rather than waiting to be read cold.

**Send the link as soon as the pull request exists, then review it (owner directive, 2026-09-16).** Opening the pull request and starting the review are one step, and neither waits on the owner. The link is what lets them open the review panel and watch the lenses land, so holding it back leaves them blind to work that is already pushed.

This replaces the earlier gate, which held the link until the review finished. That gate existed to stop a review being skipped once the work had been handed over. The replacement answers the same risk differently: the review starts in the same breath as the pull request, so no window exists in which one has happened and the other has not.

What does not change is that the review runs and that its result reaches the owner. Post the findings on the pull request, and say plainly what the review found and what it refuted, including when it found nothing. A pull request whose review never ran, or ran and never reported, is still unfinished work.

**Close a review with the line `## Review complete` (owner directive, 2026-10-06).** The build board at <https://l3a0.github.io/marketlake/>, which a GitHub Action rebuilds from GitHub, puts a pull request in the owner's queue only when it finds that line as the first line of a comment or review from the owner that is newer than the pull request's head commit. The shared board's generator, `scripts/build_board.py` in [l3a0/repo-template](https://github.com/l3a0/repo-template) at the commit `.github/workflows/board.yml` pins, holds the exact rule, which that workflow's `review-heading` input turns on. Under that rule a submitted review counts only when it names the head commit. A submitted review starts no board run, so it shows on the next run of any other kind. So once the review has run to the end, meaning every lens has reported, each finding is verified, and the fixes are pushed, the comment that closes it starts with the exact line `## Review complete`, with nothing else on that line. Only that comment carries it. A comment posted earlier says which lens is still running. Post it after the last push, and post a new one after any later push, whether a fix, a rebase, a merge from `main` or an empty commit, because the board stops counting a marker once the head moves past it. Every other label and line the board reads, and the moment a session sets each, is in the `l3a0` plugin's `/l3a0:update-build-board` skill. The skills named `/l3a0:` come from that plugin, [l3a0/claude-plugins](https://github.com/l3a0/claude-plugins), at version 0.5.0 or later, and this repository holds no copy of them. `.claude/settings.json` declares the plugin's marketplace and enables the plugin. Once the folder is trusted, Claude Code clones the marketplace and loads the plugin from it with no install step, because the plugin's source inside that marketplace is a relative path. An untrusted folder and a cloud session ignore the declaration. Nothing pins a version, so a session runs the copy its machine last fetched. A session that cannot load one of these skills says so in its report rather than skipping the step it covers.

Review by fanning out independent lenses, then verifying each finding adversarially. Several reviewers in parallel, each with one lens and no sight of the others, produce the findings. Verifiers then try to refute each one, and only what survives is acted on. Point one lens at completeness and one at over-reach, which catch the two failures that recur:

1. Fixing the instance rather than the class, such as a false claim corrected in one file while it still stands in three more.
2. Fixing past the class, such as generalising a change into places it does not belong.

**Consult the build team on every pull request review (owner directive, 2026-10-29).** Beside the completeness and over-reach lenses, run the `l3a0` plugin's `/l3a0:ask-team` in its lens mode. It seats the engineering roles the change touches as more blind lenses, such as the data engineer on a change to how the lake writes Parquet, or the operator on a change to how the daemon deploys. Its own verifiers try to refute each finding by executing, as the review's do for its other lenses, and only what survives comes back. The review folds what survives into its own comment, so the board still reads one `## Review complete` per round. The owner asked for this so a review hears from the roles whose objective a change can hurt, not only from lenses aimed at the diff.

Verify by executing, not by reading. Mutate the code and confirm a test fails. A test that still passes under mutation does not cover what it claims to cover. Say plainly what the review found and what it refuted, including when it found nothing.

**Watch the checks and fix what they find (owner directive, 2026-09-17).** A pull request is not handed over until its checks have run and settled. Pushing is not the end of the work, because the branch that passes locally is not the branch CI builds. CI builds the merge of the branch and its base, and the base moves.

So watch the run rather than assume it. `gh pr checks <n> --watch` blocks until every check settles, and `gh pr view <n> --json statusCheckRollup` says what each one concluded. A CI run takes minutes, so the watch runs in the background, per "Delegating work to subagents" above, and the main thread acts on its notification. When a check fails, read its log, fix the cause, and push again, in the same session and without waiting to be asked. A red check the owner finds first is work handed over unfinished.

Three measurements from this repository make the rule sharper than "look for a green tick".

1. **Nothing requires a check to pass.** The `Default` ruleset on `main` carries no `required_status_checks` rule, so a red job blocks no merge. Forty-three pull requests merged from 2026-09-16 onward and none of them had to pass anything. Until [#412](https://github.com/l3a0/marketlake/issues/412) closes, the check is advice, which is exactly why reading it is the session's job rather than the gate's. [#412](https://github.com/l3a0/marketlake/issues/412) closed on 2026-09-17, and the `Default` ruleset has required the `test` check to pass since then.
2. **A conflicting pull request gets no run at all.** A `pull_request` workflow builds the merge ref, and a branch that conflicts has none, so no run is created. [PR #414](https://github.com/l3a0/marketlake/pull/414) showed three green CodeQL entries and no `test` run whatsoever. An absent check reads as a short rollup rather than as a failure, so count what ran instead of scanning for red.
3. **Green goes stale.** A run is computed against one merge ref, and a later merge to the base replaces it. [PR #433](https://github.com/l3a0/marketlake/pull/433)'s checks read green after the branch had already conflicted underneath them. Re-read the rollup whenever the base has moved.

**Red goes stale the same way, and costs more.** The sibling `repo-template` measured this behavior, not this repo. A failure inherited from the base survives in the rollup after the base has been fixed. [PR #3](https://github.com/l3a0/repo-template/pull/3) there carried a red `test` check from a run computed 25 seconds before the pull request that fixed its base merged. Rebasing made it green, and a monitor reading the older snapshot reported the failure again afterwards. A red check is a claim about one merge ref at one moment, so re-read it before acting, and check whether the pull request has already merged before fixing anything.

Fix the cause rather than the symptom. A lint rule that fails on one file usually fails on its siblings, so sweep for the class. Re-running a job changes nothing the second time unless the failure was the runner rather than the code. Where a failure comes from another branch's merge rather than from this change, say so on the pull request instead of absorbing an unrelated fix into it.

**A filed issue is triaged when it is filed (owner directive, 2026-09-13, restated 2026-10-05).** Filing is not finished when the issue exists. An issue nobody triaged appears in no milestone view and no view scoped by kind, and nothing brings it back on its own. Three arrived that way in a single day, each from a session told to file what it found and nothing further: [#143](https://github.com/l3a0/marketlake/issues/143), then [#145](https://github.com/l3a0/marketlake/issues/145) and [#146](https://github.com/l3a0/marketlake/issues/146). The sessions did exactly what was asked, which is why the rule belongs here rather than in a reminder.

**Milestones exist only for MVP deliverables (owner directive, 2026-10-05).** On that date the owner closed the slice milestones and opened one for the next usable product, [MVP 2, capture on a hosted VM](https://github.com/l3a0/marketlake/milestone/5). [The open MVP milestone](https://github.com/l3a0/marketlake/milestones?state=open) is the one current work follows, and [docs/build-plan.md](docs/build-plan.md) records each MVP's dates. A milestone now says that an issue is on the path to that product, per the usable-product directive above. It no longer says which part of the build owns the issue.

So a filed issue is finished when it says three things.

1. Where it stands against the current MVP. Apply these in order and stop at the first match.
   1. An issue on the current MVP's path carries that MVP's milestone.
   2. A bug that can lose a captured minute, or a bug in an alarm, carries no milestone and no `deferred` label. Exceptions 1 and 2 of the usable-product directive never defer those.
   3. Any other issue carries no milestone and the `deferred` label.
2. A label says what kind of work it is.
3. A dependency says what it waits on, where it waits on anything.

No automation supplies the first two. A project's auto-add makes an untriaged issue visible and does not triage it, and nothing infers whether a gap is on the MVP's path.

A missing milestone no longer marks an untriaged issue, because every deferred issue and every exempt bug has none. The exempt bugs carry only the `bug` label, so the tracker cannot tell one from a bug nobody triaged. List the open issues that carry neither a milestone nor `deferred`. Each one is either a capture or alarm bug, or work nobody triaged.

```bash
gh issue list --state open --limit 500 --json number,title,labels,milestone --jq '.[] | select(.milestone == null) | select([.labels[].name] | index("deferred") | not) | "\(.number) \(.title)"'
```

The same applies to an issue a spawned session is told it may file. The instruction to file carries the instruction to triage, or the work lands where nothing will look for it.

**Close an issue only when nothing is left in it (owner directive, 2026-09-12).** Before a PR closes an issue, move whatever that PR does not do into its own issue. A piece described only inside a body goes when the body closes, and nothing surfaces it again. Two issues have already gone that way, and in both the closing PR's own text named the work it was leaving. [#101](https://github.com/l3a0/marketlake/issues/101) was closed by the PR that did half of it. [PR #85](https://github.com/l3a0/marketlake/pull/85) opened with `Closes #77.` and then said a later PR would drop the column, whose remainder survives only because [#96](https://github.com/l3a0/marketlake/issues/96) was filed for it afterwards.

While a piece is outstanding, a PR writes `Part of #NN` and the closing keyword waits for the PR that leaves nothing. GitHub reads the keyword only when the number follows it immediately, so `Closes #101` closes and `Closes the second half of #101` closes nothing at all. [PR #109](https://github.com/l3a0/marketlake/pull/109) wrote the second form and linked no issue. The keyword also has to be plain text, and a code span around it defeats it the same way. Rendered, a code span and plain text differ only in font, so reading the body back does not distinguish them. `gh pr view <n> --json closingIssuesReferences` does, and an empty result on a pull request that means to close something is the signal to fix the body before merging.

An issue whose pieces have all been split has no finishing PR left, so close it by hand and name where each piece went. Do the same when two PRs are open against one issue, because merge order decides which lands last and neither body can know it. The split is the guard that matters here. The keyword discipline only keeps the issue open long enough to make the split.

A split leaves code comments pointing at the parent for work that moved, so repoint those in the PR that splits. A comment naming a closed issue in the past tense records what happened rather than pointing anywhere, and it stays. An unstarted piece goes to an issue, not to a document. The issue carries that piece's scope and its status together, per the directive at the top of this file, so no copy elsewhere goes stale the moment work lands.

PR titles use a Conventional Commits prefix. The form is `type(scope): summary`. `gh pr list --state merged --json title` reports which prefixes this repo has used, rather than a list here that goes stale on the first unfamiliar one. Add a scope in parens when it sharpens the title, like `docs(CLAUDE.md)`. Drop it when none does, like a plain `docs:` for a whole-doc change. The form and the scope rule match the sibling `trading-strategies` repo. Its fixed list leaves out `refactor` and `test`, which this repo uses and that one does not.

PR bodies use Markdown section headings, not a wall of prose. Lead with `## Why`, then `## What`. Add situational sections after as the change needs them, like `## Scope`, `## Notes`, or `## Evidence`. The body's prose obeys the writing-style rules above. So clear, short sentences and no em dashes, even though the sibling repo allows them. End every body with the footer line: `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.
